package stream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	"github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1/apiv1alpha1connect"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
)

// The generated Connect handler serves both unary and streaming RPCs. For
// focused tests we embed its unimplemented unary methods and delegate only
// the book streaming method to the real implementation.
type bookTestService struct {
	apiv1alpha1connect.UnimplementedMarketDataServiceHandler
	book *BookRPC
}

func (s *bookTestService) StreamOrderBook(ctx context.Context, req *connect.Request[api.StreamOrderBookRequest], out *connect.ServerStream[api.StreamOrderBookResponse]) error {
	return s.book.StreamOrderBook(ctx, req, out)
}

func newBookTestClient(t *testing.T, src Source, queue, max int) (*Hub, *BookRPC, apiv1alpha1connect.MarketDataServiceClient, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	hub, err := NewHub(ctx, src, 50*time.Millisecond, time.Second, queue)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBookRPC(wsRegistry(t), src, hub, max)
	if err != nil {
		t.Fatal(err)
	}
	path, handler := apiv1alpha1connect.NewMarketDataServiceHandler(&bookTestService{book: b})
	mux := http.NewServeMux()
	mux.Handle(path, WithBookWriteDeadlines(handler))
	server := httptest.NewServer(mux)
	client := apiv1alpha1connect.NewMarketDataServiceClient(server.Client(), server.URL)
	return hub, b, client, func() { cancel(); server.Close() }
}

func mustReceiveBook(t *testing.T, stream *connect.ServerStreamForClient[api.StreamOrderBookResponse]) *api.StreamOrderBookResponse {
	t.Helper()
	if !stream.Receive() {
		t.Fatalf("receive: %v", stream.Err())
	}
	return stream.Msg()
}

func TestConnectOrderBookSnapshotThenContiguousDeltas(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1, committed: []Result{mkResult(2), mkResult(3)}}
	_, _, client, cleanup := newBookTestClient(t, src, 16, 4)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := client.StreamOrderBook(ctx, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: "BTC-USDT", Depth: 20}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := mustReceiveBook(t, s)
	if first.GetSnapshot() == nil || first.GetSnapshot().GetMarketSequence() != 1 {
		t.Fatalf("first event must be snapshot at 1: %v", first)
	}
	for _, seq := range []uint64{2, 3} {
		msg := mustReceiveBook(t, s)
		if msg.GetDelta() == nil || msg.GetDelta().GetMarketSequence() != seq || msg.GetDelta().GetPreviousMarketSequence() != seq-1 {
			t.Fatalf("expected contiguous delta %d: %v", seq, msg)
		}
	}
}

func TestConnectOrderBookRejectsMarketAndDepth(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1}
	_, _, client, cleanup := newBookTestClient(t, src, 16, 4)
	defer cleanup()
	for _, tc := range []struct {
		name, market string
		depth        uint32
		want         connect.Code
	}{
		{"missing market", "", 20, connect.CodeInvalidArgument},
		{"unknown market", "ETH-USDT", 20, connect.CodeNotFound},
		{"zero depth", "BTC-USDT", 0, connect.CodeInvalidArgument},
		{"unmaterialized depth", "BTC-USDT", 10, connect.CodeInvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			stream, err := client.StreamOrderBook(ctx, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: tc.market, Depth: tc.depth}))
			if err != nil {
				if connect.CodeOf(err) != tc.want {
					t.Fatalf("got %v, want %v", err, tc.want)
				}
				return
			}
			defer stream.Close()
			if stream.Receive() {
				t.Fatalf("invalid request accepted: %v", stream.Msg())
			}
			if got := connect.CodeOf(stream.Err()); got != tc.want {
				t.Fatalf("got %v (%v), want %v", got, stream.Err(), tc.want)
			}
		})
	}
}

func TestConnectOrderBookSequenceGapIsTerminal(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1, committed: []Result{mkResult(3)}}
	_, _, client, cleanup := newBookTestClient(t, src, 16, 4)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := client.StreamOrderBook(ctx, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: "BTC-USDT", Depth: 20}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if mustReceiveBook(t, s).GetSnapshot() == nil {
		t.Fatal("missing snapshot")
	}
	if s.Receive() {
		t.Fatalf("must not deliver noncontiguous delta: %v", s.Msg())
	}
	if got := connect.CodeOf(s.Err()); got != connect.CodeUnavailable {
		t.Fatalf("gap code %v (%v)", got, s.Err())
	}
}

func TestConnectOrderBookCapAndDisconnectRelease(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1}
	hub, _, client, cleanup := newBookTestClient(t, src, 8, 1)
	defer cleanup()
	ctx1, cancel1 := context.WithCancel(context.Background())
	stream1, err := client.StreamOrderBook(ctx1, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: "BTC-USDT", Depth: 20}))
	if err != nil {
		t.Fatal(err)
	}
	if mustReceiveBook(t, stream1).GetSnapshot() == nil {
		t.Fatal("missing initial snapshot")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	s2, err := client.StreamOrderBook(ctx2, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: "BTC-USDT", Depth: 20}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Fatal(err)
		}
	} else {
		if s2.Receive() {
			t.Fatal("over capacity stream accepted")
		}
		if got := connect.CodeOf(s2.Err()); got != connect.CodeResourceExhausted {
			t.Fatalf("cap: %v, %v", got, s2.Err())
		}
		_ = s2.Close()
	}
	cancel2()
	cancel1()
	_ = stream1.Close()
	// HTTP cancel must release hub subscription and the stream capacity slot.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		remaining := len(hub.markets)
		hub.mu.Unlock()
		if remaining == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	hub.mu.Lock()
	remaining := len(hub.markets)
	hub.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("orphaned hub state: %d markets", remaining)
	}
}

func TestConnectOrderBookHeartbeatDoesNotAdvanceSequence(t *testing.T) {
	src := &fakeSource{snapshotSeq: 17}
	_, b, client, cleanup := newBookTestClient(t, src, 8, 1)
	defer cleanup()
	b.heartbeatInterval = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := client.StreamOrderBook(ctx, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: "BTC-USDT", Depth: 20}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if mustReceiveBook(t, s).GetSnapshot().GetMarketSequence() != 17 {
		t.Fatal("missing snapshot")
	}
	msg := mustReceiveBook(t, s)
	if msg.GetHeartbeat() == nil || msg.GetHeartbeat().GetLastDeliveredMarketSequence() != 17 {
		t.Fatalf("heartbeat should not advance committed cursor: %v", msg)
	}
}

func TestConnectOrderBookInvalidSnapshotIsFailClosed(t *testing.T) {
	src := &fakeSource{snapshotSeq: 0} // durable projection has not started
	_, _, client, cleanup := newBookTestClient(t, src, 8, 1)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := client.StreamOrderBook(ctx, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: "BTC-USDT", Depth: 20}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeUnavailable {
			t.Fatal(err)
		}
		return
	}
	defer s.Close()
	if s.Receive() {
		t.Fatal("zero-sequence snapshot accepted")
	}
	if connect.CodeOf(s.Err()) != connect.CodeUnavailable {
		t.Fatalf("unexpected failure: %v", s.Err())
	}
}

func TestHubBookOnlyFiltersSoftAndRetainsOnlyDelta(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{snapshotSeq: 1}
	hub, _ := NewHub(ctx, src, 50*time.Millisecond, time.Second, 8)
	sub, err := hub.SubscribeBook(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	hub.mu.Lock()
	state := hub.markets[7]
	hub.mu.Unlock()
	hub.broadcast(state, Feed{Soft: &Soft{Ticker: &md.Ticker{MarketId: "BTC-USDT"}}})
	hub.broadcast(state, Feed{Result: &Result{Delta: mkResult(2).Delta, Trades: []*md.TradeTick{{MarketId: "BTC-USDT"}}}})
	select {
	case got := <-sub.Messages:
		if got.Result == nil || got.Result.Delta.GetMarketSequence() != 2 || got.Result.Trades != nil || got.Soft != nil {
			t.Fatalf("book feed should contain only delta: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("missing book update")
	}
	select {
	case got := <-sub.Messages:
		t.Fatalf("unexpected non-book update %+v", got)
	default:
	}
}

// A missing HTTP deadline layer must never silently enable unbounded writes.
func TestConnectOrderBookRequiresWriteDeadlineMiddleware(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{snapshotSeq: 1}
	hub, err := NewHub(ctx, src, 50*time.Millisecond, time.Second, 8)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBookRPC(wsRegistry(t), src, hub, 1)
	if err != nil {
		t.Fatal(err)
	}
	path, handler := apiv1alpha1connect.NewMarketDataServiceHandler(&bookTestService{book: b})
	mux := http.NewServeMux()
	mux.Handle(path, handler) // intentionally lacks WithBookWriteDeadlines
	server := httptest.NewServer(mux)
	defer server.Close()
	client := apiv1alpha1connect.NewMarketDataServiceClient(server.Client(), server.URL)
	requestCtx, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	stream, err := client.StreamOrderBook(requestCtx, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: "BTC-USDT", Depth: 20}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeUnavailable {
			t.Fatal(err)
		}
		return
	}
	defer stream.Close()
	if stream.Receive() {
		t.Fatal("unbounded streaming handler accepted")
	}
	if connect.CodeOf(stream.Err()) != connect.CodeUnavailable {
		t.Fatalf("error: %v", stream.Err())
	}
}

func TestBookErrorCodes(t *testing.T) {
	cases := []struct {
		err  error
		code connect.Code
	}{
		{ErrSlowConsumer, connect.CodeResourceExhausted},
		{ErrSequenceGap, connect.CodeUnavailable},
		{context.Canceled, connect.CodeCanceled},
		{context.DeadlineExceeded, connect.CodeDeadlineExceeded},
	}
	for _, tc := range cases {
		if code := connect.CodeOf(bookRPCError(tc.err)); code != tc.code {
			t.Errorf("%v: got %v, want %v", tc.err, code, tc.code)
		}
	}
}
