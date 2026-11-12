package stream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	"github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1/apiv1alpha1connect"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// tickerFixture is a concurrency-safe durable journal plus independently
// observable latest ticker. The same methods are used by hub and RPC reads.
type tickerFixture struct {
	mu          sync.Mutex
	seq         uint64
	rows        []Result
	latest      *md.Ticker
	tickerReads int
	tickerErr   error
}

func fixtureTicker(seq uint64) *md.Ticker {
	bid, ask := uint64(100), uint64(110)
	return &md.Ticker{MarketId: "BTC-USDT", MarketSequence: seq,
		BestBidPriceUnits: &bid, BestAskPriceUnits: &ask, UpdatedAt: timestamppb.New(time.Unix(1, 0))}
}
func newTickerFixture() *tickerFixture {
	return &tickerFixture{seq: 1, latest: fixtureTicker(1)}
}
func (f *tickerFixture) commitThrough(seq uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for next := f.seq + 1; next <= seq; next++ {
		f.rows = append(f.rows, mkResult(next))
	}
	f.seq = seq
	f.latest = fixtureTicker(seq)
}
func (f *tickerFixture) setTicker(t *md.Ticker, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latest, f.tickerErr = t, err
}
func (f *tickerFixture) LatestTicker(_ context.Context, _ marketregistry.Market) (*md.Ticker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tickerReads++
	if f.tickerErr != nil {
		return nil, f.tickerErr
	}
	if f.latest == nil {
		return nil, nil
	}
	return proto.Clone(f.latest).(*md.Ticker), nil
}
func (f *tickerFixture) Snapshot(_ context.Context, m marketregistry.Market) (*md.BookSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &md.BookSnapshot{MarketId: m.Symbol, SourceMarketId: m.SourceID,
		MarketSequence: f.seq, MarketDefinitionVersion: m.DefinitionVersion, Depth: m.Depth}, nil
}
func (f *tickerFixture) Head(_ context.Context, _ marketregistry.Market) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seq, nil
}
func (f *tickerFixture) ReplayFloor(context.Context, marketregistry.Market) (uint64, error) {
	return 1, nil
}
func (f *tickerFixture) Results(_ context.Context, _ marketregistry.Market, after, through uint64, limit int) ([]Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []Result
	for _, row := range f.rows {
		seq := row.Delta.GetMarketSequence()
		if seq > after && (through == 0 || seq <= through) {
			result = append(result, row)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}
func (f *tickerFixture) Current(context.Context, marketregistry.Market, time.Time) (*Soft, error) {
	return &Soft{Ticker: fixtureTicker(1)}, nil
}

type tickerTestService struct {
	apiv1alpha1connect.UnimplementedMarketDataServiceHandler
	ticker *TickerRPC
}

func (s *tickerTestService) StreamTicker(ctx context.Context, req *connect.Request[api.StreamTickerRequest], out *connect.ServerStream[api.StreamTickerResponse]) error {
	return s.ticker.StreamTicker(ctx, req, out)
}
func newTickerTestClient(t *testing.T, src Source, queue, max int) (*Hub, *TickerRPC, apiv1alpha1connect.MarketDataServiceClient, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	hub, err := NewHub(ctx, src, 50*time.Millisecond, time.Second, queue)
	if err != nil {
		t.Fatal(err)
	}
	ticker, err := NewTickerRPC(wsRegistry(t), hub, max)
	if err != nil {
		t.Fatal(err)
	}
	path, handler := apiv1alpha1connect.NewMarketDataServiceHandler(&tickerTestService{ticker: ticker})
	mux := http.NewServeMux()
	mux.Handle(path, WithStreamWriteDeadlines(handler))
	server := httptest.NewServer(mux)
	client := apiv1alpha1connect.NewMarketDataServiceClient(server.Client(), server.URL)
	return hub, ticker, client, func() {
		cancel()
		_ = ticker.ShutdownAndWait(context.Background())
		_ = hub.Shutdown(context.Background())
		server.Close()
	}
}
func tickerEvent(t *testing.T, s *connect.ServerStreamForClient[api.StreamTickerResponse]) *api.StreamTickerResponse {
	t.Helper()
	if !s.Receive() {
		t.Fatalf("receive ticker: %v", s.Err())
	}
	return s.Msg()
}
func tickerNextUpdate(t *testing.T, s *connect.ServerStreamForClient[api.StreamTickerResponse]) *md.Ticker {
	t.Helper()
	for {
		msg := tickerEvent(t, s)
		if msg.GetUpdate() != nil {
			return msg.GetUpdate()
		}
		if msg.GetHeartbeat() == nil {
			t.Fatalf("unexpected ticker event: %v", msg)
		}
	}
}

func TestTickerSnapshotMonotonicLatestStateAndReconnect(t *testing.T) {
	src := newTickerFixture()
	_, _, client, cleanup := newTickerTestClient(t, src, 32, 3)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := client.StreamTicker(ctx, connect.NewRequest(&api.StreamTickerRequest{MarketId: "BTC-USDT"}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := tickerEvent(t, s)
	if first.GetSnapshot() == nil || first.GetSnapshot().GetMarketSequence() != 1 || first.GetMarketDefinitionVersion() != 3 {
		t.Fatalf("missing initial durable snapshot: %+v", first)
	}
	// A ticker is LATEST state, not a contiguous book delta or trade log.
	src.commitThrough(4)
	got := tickerNextUpdate(t, s)
	if got.GetMarketSequence() != 4 {
		t.Fatalf("got sequence %d, want coalesced head 4", got.GetMarketSequence())
	}
	// Reconnecting replaces the prior state rather than replaying sequences 2-4.
	fresh, err := client.StreamTicker(ctx, connect.NewRequest(&api.StreamTickerRequest{MarketId: "BTC-USDT"}))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	second := tickerEvent(t, fresh)
	if second.GetSnapshot() == nil || second.GetSnapshot().GetMarketSequence() != 4 {
		t.Fatalf("reconnect must start with new durable snapshot: %+v", second)
	}
	src.commitThrough(5)
	if next := tickerNextUpdate(t, fresh); next.GetMarketSequence() != 5 {
		t.Fatalf("unexpected next sequence: %+v", next)
	}
}

func TestTickerHubCoalescesPendingStateAndReclaimsBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := newTickerFixture()
	hub, err := NewHub(ctx, src, 10*time.Second, time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Shutdown(context.Background())
	sub, err := hub.SubscribeTicker(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	hub.mu.Lock()
	state := hub.markets[7]
	hub.mu.Unlock()
	for seq := uint64(2); seq < 200; seq++ {
		hub.broadcast(state, Feed{Ticker: fixtureTicker(seq)})
	}
	select {
	case err := <-sub.Errors:
		t.Fatalf("coalescing must not disconnect lagging ticker consumer: %v", err)
	case msg := <-sub.Messages:
		if msg.Ticker == nil || msg.Ticker.GetMarketSequence() != 199 {
			t.Fatalf("latest state lost: %+v", msg.Ticker)
		}
		msg.Release()
	case <-time.After(time.Second):
		t.Fatal("missing ticker")
	}
	hub.mu.Lock()
	outstanding := hub.queuedBytes
	hub.mu.Unlock()
	if outstanding != 0 {
		t.Fatalf("leaked queued byte credits: %d", outstanding)
	}
	// WebSocket's old five-second soft ticker must never enter the RPC lane.
	book := mkResult(200)
	hub.broadcast(state, Feed{Result: &book})
	hub.broadcast(state, Feed{Soft: &Soft{Ticker: fixtureTicker(200)}})
	select {
	case m := <-sub.Messages:
		m.Release()
		t.Fatalf("soft update leaked to Connect ticker: %+v", m)
	default:
	}
}

func TestTickerHubFanoutOnePollNotOneQueryPerClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := newTickerFixture()
	hub, _ := NewHub(ctx, src, 10*time.Second, time.Second, 4)
	defer hub.Shutdown(context.Background())
	var subs []*Subscription
	for i := 0; i < 8; i++ {
		sub, err := hub.SubscribeTicker(ctx, mkMarket())
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
		defer sub.Close()
	}
	src.commitThrough(2)
	hub.mu.Lock()
	state := hub.markets[7]
	hub.mu.Unlock()
	hub.refreshTicker(ctx, state) // one market read, not eight subscriber reads
	for _, sub := range subs {
		deadline := time.After(2 * time.Second)
		for {
			select {
			case msg := <-sub.Messages:
				msg.Release()
				if msg.Ticker == nil {
					t.Fatal("non ticker feed")
				}
				if msg.Ticker.GetMarketSequence() == 2 {
					goto nextSubscriber
				}
			case <-deadline:
				t.Fatal("did not fan out head ticker")
			}
		}
	nextSubscriber:
	}
	src.mu.Lock()
	count := src.tickerReads
	src.mu.Unlock()
	if count != 1 {
		t.Fatalf("ticker was fetched per subscriber, not per market: %d", count)
	}
}

func TestTickerRejectsInvalidRequests(t *testing.T) {
	_, _, client, cleanup := newTickerTestClient(t, newTickerFixture(), 4, 2)
	defer cleanup()
	for _, tc := range []struct {
		market string
		code   connect.Code
	}{
		{"", connect.CodeInvalidArgument}, {"UNKNOWN", connect.CodeNotFound},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		s, err := client.StreamTicker(ctx, connect.NewRequest(&api.StreamTickerRequest{MarketId: tc.market}))
		if err == nil {
			if s.Receive() {
				t.Fatalf("accepted invalid market %q", tc.market)
			}
			err = s.Err()
			_ = s.Close()
		}
		if connect.CodeOf(err) != tc.code {
			t.Fatalf("%q: got %v want %v", tc.market, err, tc.code)
		}
		cancel()
	}
}

func TestTickerFailsClosedOnDurableReadFailure(t *testing.T) {
	src := newTickerFixture()
	_, _, client, cleanup := newTickerTestClient(t, src, 4, 2)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := client.StreamTicker(ctx, connect.NewRequest(&api.StreamTickerRequest{MarketId: "BTC-USDT"}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if tickerEvent(t, s).GetSnapshot() == nil {
		t.Fatal("missing snapshot")
	}
	src.setTicker(nil, errors.New("db unavailable"))
	for s.Receive() { /* must terminate, not deliver stale ticker indefinitely */
	}
	if connect.CodeOf(s.Err()) != connect.CodeUnavailable {
		t.Fatalf("read failure code: %v", s.Err())
	}
}

func TestTickerInvalidAndRegressedObservations(t *testing.T) {
	src := newTickerFixture()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub, _ := NewHub(ctx, src, 10*time.Second, time.Second, 2)
	defer hub.Shutdown(context.Background())
	sub, err := hub.SubscribeTicker(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	// Set a published watermark and force an older authoritative observation.
	hub.mu.Lock()
	state := hub.markets[7]
	hub.mu.Unlock()
	state.tickerSequence = 10 // before first worker poll
	hub.refreshTicker(ctx, state)
	select {
	case e := <-sub.Errors:
		if !errors.Is(e, ErrSequenceGap) {
			t.Fatalf("unexpected error: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("regression was not rejected")
	}
	if validateStreamTicker(&md.Ticker{MarketId: "BTC-USDT", MarketSequence: 1}, mkMarket()) == nil {
		t.Fatal("accepted ticker without durable timestamp")
	}
}

func TestTickerShutdownJoinsAndClosesAdmission(t *testing.T) {
	src := newTickerFixture()
	_, ticker, client, cleanup := newTickerTestClient(t, src, 2, 1)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := client.StreamTicker(ctx, connect.NewRequest(&api.StreamTickerRequest{MarketId: "BTC-USDT"}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if tickerEvent(t, s).GetSnapshot() == nil {
		t.Fatal("missing snapshot")
	}
	// An accepted stream consumes the single capacity slot.
	second, err := client.StreamTicker(ctx, connect.NewRequest(&api.StreamTickerRequest{MarketId: "BTC-USDT"}))
	if err == nil {
		if second.Receive() {
			t.Fatal("capacity limit ignored")
		}
		err = second.Err()
		_ = second.Close()
	}
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("capacity: %v", err)
	}
	if err := ticker.ShutdownAndWait(ctx); err != nil {
		t.Fatalf("shutdown did not join stream: %v", err)
	}
	if err := ticker.ShutdownAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	if ticker.active != 0 {
		t.Fatalf("active streams after shutdown: %d", ticker.active)
	}
	if e := ticker.enter(); !errors.Is(e, ErrStreamShutdown) {
		t.Fatalf("reopened admission: %v", e)
	}
}

func TestTickerRecordedAtExactNanoseconds(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		sec  int64
		nano int
	}{
		{"1234567890", 1, 234567890}, {"-1", -1, 999999999}, {"-1000000001", -2, 999999999},
	} {
		ts, err := tickerRecordedAt(tc.raw)
		if err != nil {
			t.Fatal(err)
		}
		if ts.AsTime().Unix() != tc.sec || ts.AsTime().Nanosecond() != tc.nano {
			t.Fatalf("%s got %s", tc.raw, ts.AsTime())
		}
	}
	for _, bad := range []string{"abc", "9999999999999999999999999999999999999999999999999999"} {
		if _, err := tickerRecordedAt(bad); err == nil {
			t.Fatalf("accepted timestamp %q", bad)
		}
	}
}

func TestTickerHeartbeatDoesNotAdvanceMarketSequence(t *testing.T) {
	src := newTickerFixture()
	_, rpc, client, cleanup := newTickerTestClient(t, src, 4, 2)
	defer cleanup()
	rpc.heartbeatInterval = 30 * time.Millisecond // set before new stream admission
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := client.StreamTicker(ctx, connect.NewRequest(&api.StreamTickerRequest{MarketId: "BTC-USDT"}))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if first := tickerEvent(t, s); first.GetSnapshot() == nil || first.GetSnapshot().GetMarketSequence() != 1 {
		t.Fatalf("expected initial snapshot: %v", first)
	}
	var heartbeat *api.StreamTickerResponse
	for heartbeat == nil {
		msg := tickerEvent(t, s)
		if msg.GetHeartbeat() != nil {
			heartbeat = msg
		} else if msg.GetUpdate() != nil {
			t.Fatalf("invented update without a durable commit: %v", msg)
		}
	}
	if heartbeat.GetHeartbeat().GetLastDeliveredMarketSequence() != 1 || heartbeat.GetMarketDefinitionVersion() != 3 {
		t.Fatalf("heartbeat must carry delivered cursor only: %v", heartbeat)
	}
	src.commitThrough(2)
	if update := tickerNextUpdate(t, s); update.GetMarketSequence() != 2 {
		t.Fatalf("heartbeat illegally advanced the cursor: %v", update)
	}
}

func TestTickerHubEnforcesPayloadLimitWithoutByteCreditLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := newTickerFixture()
	hub, err := NewHub(ctx, src, 10*time.Second, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Shutdown(context.Background())
	sub, err := hub.SubscribeTicker(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	hub.mu.Lock()
	state := hub.markets[7]
	hub.mu.Unlock()
	oversized := fixtureTicker(2)
	addUnknownPayload(oversized, MaxFeedBytes)
	hub.broadcast(state, Feed{Ticker: oversized})
	select {
	case e := <-sub.Errors:
		if !errors.Is(e, ErrPayloadTooLarge) {
			t.Fatalf("wrong overflow error: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("oversized ticker not rejected")
	}
	hub.mu.Lock()
	bytes := hub.queuedBytes
	hub.mu.Unlock()
	if bytes != 0 {
		t.Fatalf("oversized ticker retained %d bytes", bytes)
	}
}

func TestTickerDoesNotPolluteBookOrTradeSubscriptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := newTickerFixture()
	hub, err := NewHub(ctx, src, 10*time.Second, time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Shutdown(context.Background())
	book, err := hub.SubscribeBook(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer book.Close()
	trades, err := hub.SubscribeTrades(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer trades.Close()
	ticker, err := hub.SubscribeTicker(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer ticker.Close()
	hub.mu.Lock()
	state := hub.markets[7]
	hub.mu.Unlock()
	hub.broadcast(state, Feed{Ticker: fixtureTicker(2)})
	select {
	case feed := <-ticker.Messages:
		feed.Release()
		if feed.Ticker == nil || feed.Ticker.GetMarketSequence() != 2 {
			t.Fatalf("incorrect ticker: %+v", feed)
		}
	default:
		t.Fatal("ticker event lost")
	}
	select {
	case feed := <-book.Messages:
		feed.Release()
		t.Fatal("ticker incorrectly delivered to book subscriber")
	default:
	}
	select {
	case feed := <-trades.Messages:
		feed.Release()
		t.Fatal("ticker incorrectly delivered to trades subscriber")
	default:
	}
}
