package stream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	"github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1/apiv1alpha1connect"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
)

type tradeTestService struct {
	apiv1alpha1connect.UnimplementedMarketDataServiceHandler
	trade *TradeRPC
}

func (s *tradeTestService) StreamTrades(ctx context.Context, req *connect.Request[api.StreamTradesRequest], out *connect.ServerStream[api.StreamTradesResponse]) error {
	return s.trade.StreamTrades(ctx, req, out)
}
func tradeTick(seq, ordinal uint64) *md.TradeTick {
	return &md.TradeTick{MarketId: "BTC-USDT", SourceMarketId: 7, MatchSequence: seq, EventOrdinal: ordinal,
		MarketDefinitionVersion: 3, TradeId: tradeID(seq, ordinal), PriceUnits: 100, BaseQuantityAtoms: 10, QuoteQuantityAtoms: 1000}
}
func tradeID(seq, ordinal uint64) string { // canonical numeric provenance
	return "7:" + utoa(seq) + ":" + utoa(ordinal)
}
func utoa(n uint64) string { return strconv.FormatUint(n, 10) }
func mkTradeResult(seq uint64, ordinals ...uint64) Result {
	r := mkResult(seq)
	for _, ord := range ordinals {
		r.Trades = append(r.Trades, tradeTick(seq, ord))
	}
	return r
}
func newTradeTestClient(t *testing.T, src Source, queue, max int) (*Hub, *TradeRPC, apiv1alpha1connect.MarketDataServiceClient, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	hub, e := NewHub(ctx, src, 50*time.Millisecond, time.Second, queue)
	if e != nil {
		t.Fatal(e)
	}
	tr, e := NewTradeRPC(wsRegistry(t), src, hub, max)
	if e != nil {
		t.Fatal(e)
	}
	path, handler := apiv1alpha1connect.NewMarketDataServiceHandler(&tradeTestService{trade: tr})
	mux := http.NewServeMux()
	mux.Handle(path, WithStreamWriteDeadlines(handler))
	server := httptest.NewServer(mux)
	client := apiv1alpha1connect.NewMarketDataServiceClient(server.Client(), server.URL)
	return hub, tr, client, func() { cancel(); server.Close() }
}
func mustTradeEvent(t *testing.T, stream *connect.ServerStreamForClient[api.StreamTradesResponse]) *api.StreamTradesResponse {
	t.Helper()
	if !stream.Receive() {
		t.Fatalf("trade receive: %v", stream.Err())
	}
	return stream.Msg()
}
func completeTradeCursor(seq uint64) *api.TradeStreamCursor {
	return &api.TradeStreamCursor{MarketSequence: seq, MarketDefinitionVersion: 3, SequenceComplete: true}
}
func TestStreamTradesReplaysWholeResultsInOrdinalOrder(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1, committed: []Result{mkTradeResult(2, 0, 3), mkTradeResult(3), mkTradeResult(4, 0)}}
	_, _, client, cleanup := newTradeTestClient(t, src, 16, 2)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, e := client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT", ResumeAfter: completeTradeCursor(1)}))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	started := mustTradeEvent(t, s).GetStarted()
	if started == nil || started.GetMode() != api.StreamTradesStarted_MODE_REPLAY || started.GetHeadMarketSequence() != 4 || started.GetMarketDefinitionVersion() != 3 {
		t.Fatalf("start: %+v", started)
	}
	for _, want := range []struct{ seq, ord uint64 }{{2, 0}, {2, 3}} {
		got := mustTradeEvent(t, s).GetTrade()
		if got == nil || got.GetMatchSequence() != want.seq || got.GetEventOrdinal() != want.ord || got.GetTradeId() != tradeID(want.seq, want.ord) {
			t.Fatalf("trade: %+v", got)
		}
	}
	if end := mustTradeEvent(t, s).GetSequenceEnd(); end == nil || end.GetMarketSequence() != 2 {
		t.Fatalf("sequence end: %+v", end)
	}
	if tr := mustTradeEvent(t, s).GetTrade(); tr == nil || tr.GetMatchSequence() != 4 {
		t.Fatalf("trade 4: %+v", tr)
	}
	if end := mustTradeEvent(t, s).GetSequenceEnd(); end == nil || end.GetMarketSequence() != 4 {
		t.Fatalf("sequence end 4: %+v", end)
	}
}
func TestStreamTradesPartialCursorDoesNotDuplicateOrSkip(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1, committed: []Result{mkTradeResult(2, 0, 1, 5), mkTradeResult(3), mkTradeResult(4, 0)}}
	_, _, client, cleanup := newTradeTestClient(t, src, 16, 2)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cur := &api.TradeStreamCursor{MarketSequence: 2, EventOrdinal: 1, MarketDefinitionVersion: 3}
	s, e := client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT", ResumeAfter: cur}))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if mustTradeEvent(t, s).GetStarted() == nil {
		t.Fatal("missing start")
	}
	tr := mustTradeEvent(t, s).GetTrade()
	if tr == nil || tr.GetEventOrdinal() != 5 || tr.GetMatchSequence() != 2 {
		t.Fatalf("must resume after ordinal 1: %v", tr)
	}
	if end := mustTradeEvent(t, s).GetSequenceEnd(); end == nil || end.GetMarketSequence() != 2 {
		t.Fatalf("sequence end: %v", end)
	}
	if tr := mustTradeEvent(t, s).GetTrade(); tr == nil || tr.GetMatchSequence() != 4 {
		t.Fatalf("missing trade at seq 4: %v", tr)
	}
}
func TestStreamTradesRejectsMissingStaleAndInventedCursor(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1, floor: 2, committed: []Result{mkTradeResult(2, 0, 2), mkTradeResult(3)}}
	_, _, client, cleanup := newTradeTestClient(t, src, 16, 2)
	defer cleanup()
	for _, tc := range []struct {
		name, market string
		cur          *api.TradeStreamCursor
		code         connect.Code
	}{
		{"missing market", "", nil, connect.CodeInvalidArgument},
		{"unknown market", "X-Y", nil, connect.CodeNotFound},
		{"wrong version", "BTC-USDT", &api.TradeStreamCursor{MarketSequence: 2, MarketDefinitionVersion: 2, SequenceComplete: true}, connect.CodeInvalidArgument},
		{"future cursor", "BTC-USDT", completeTradeCursor(10), connect.CodeInvalidArgument},
		{"nonzero completed ordinal", "BTC-USDT", &api.TradeStreamCursor{MarketSequence: 2, EventOrdinal: 1, MarketDefinitionVersion: 3, SequenceComplete: true}, connect.CodeInvalidArgument},
		{"nonexistent trade ordinal", "BTC-USDT", &api.TradeStreamCursor{MarketSequence: 2, EventOrdinal: 1, MarketDefinitionVersion: 3}, connect.CodeInvalidArgument},
		{"outside retention", "BTC-USDT", &api.TradeStreamCursor{MarketSequence: 1, MarketDefinitionVersion: 3}, connect.CodeFailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			s, e := client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: tc.market, ResumeAfter: tc.cur}))
			if e != nil {
				if connect.CodeOf(e) != tc.code {
					t.Fatalf("got %v want %v", e, tc.code)
				}
				return
			}
			defer s.Close()
			if s.Receive() {
				t.Fatalf("unexpected accepted stream: %v", s.Msg())
			}
			if got := connect.CodeOf(s.Err()); got != tc.code {
				t.Fatalf("got %v (%v), want %v", got, s.Err(), tc.code)
			}
		})
	}
}
func TestStreamTradesReplayRangeAndRetentionAreExplicit(t *testing.T) {
	src := &fakeSource{snapshotSeq: MaxTradeReplaySequences + 100, floor: 100}
	_, _, client, cleanup := newTradeTestClient(t, src, 8, 2)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Well before floor-1: cannot silently switch to LIVE.
	s, e := client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT", ResumeAfter: completeTradeCursor(1)}))
	if e == nil {
		defer s.Close()
		if s.Receive() {
			t.Fatal("old cursor accepted")
		}
		e = s.Err()
	}
	if connect.CodeOf(e) != connect.CodeFailedPrecondition {
		t.Fatalf("retention: %v", e)
	}
	// Within the retention floor but too far from current head.
	s, e = client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT", ResumeAfter: completeTradeCursor(99)}))
	if e == nil {
		defer s.Close()
		if s.Receive() {
			t.Fatal("unbounded replay accepted")
		}
		e = s.Err()
	}
	if connect.CodeOf(e) != connect.CodeFailedPrecondition {
		t.Fatalf("window: %v", e)
	}
}

// mutable source allows a deterministic fresh subscription with a result
// committed after the first Started frame, without test data races.
type mutableTradeSource struct {
	mu       sync.Mutex
	snapshot uint64
	rows     []Result
}

func (m *mutableTradeSource) append(r Result) { m.mu.Lock(); m.rows = append(m.rows, r); m.mu.Unlock() }
func (m *mutableTradeSource) Snapshot(_ context.Context, market marketregistry.Market) (*md.BookSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &md.BookSnapshot{MarketId: market.Symbol, SourceMarketId: market.SourceID, MarketSequence: m.snapshot, Depth: market.Depth, MarketDefinitionVersion: market.DefinitionVersion}, nil
}
func (m *mutableTradeSource) Head(context.Context, marketregistry.Market) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	head := m.snapshot
	for _, r := range m.rows {
		if r.Delta.GetMarketSequence() > head {
			head = r.Delta.GetMarketSequence()
		}
	}
	return head, nil
}
func (m *mutableTradeSource) ReplayFloor(context.Context, marketregistry.Market) (uint64, error) {
	return 1, nil
}
func (m *mutableTradeSource) Results(_ context.Context, _ marketregistry.Market, after, through uint64, limit int) ([]Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Result
	for _, r := range m.rows {
		seq := r.Delta.GetMarketSequence()
		if seq > after && (through == 0 || seq <= through) {
			out = append(out, r)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}
func (m *mutableTradeSource) Current(context.Context, marketregistry.Market, time.Time) (*Soft, error) {
	return &Soft{}, nil
}
func TestStreamTradesFreshStartAndLiveFanout(t *testing.T) {
	src := &mutableTradeSource{snapshot: 1}
	hub, _, client, cleanup := newTradeTestClient(t, src, 16, 2)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, e := client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT"}))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	start := mustTradeEvent(t, s).GetStarted()
	if start == nil || start.GetMode() != api.StreamTradesStarted_MODE_LIVE || start.GetHeadMarketSequence() != 1 {
		t.Fatalf("fresh cursor: %+v", start)
	}
	src.append(mkTradeResult(2)) // no trade; should not occupy a queue slot
	src.append(mkTradeResult(3, 0, 1))
	tr := mustTradeEvent(t, s).GetTrade()
	if tr == nil || tr.GetMatchSequence() != 3 || tr.GetEventOrdinal() != 0 {
		t.Fatalf("first live trade: %+v", tr)
	}
	tr = mustTradeEvent(t, s).GetTrade()
	if tr == nil || tr.GetMatchSequence() != 3 || tr.GetEventOrdinal() != 1 {
		t.Fatalf("second live trade: %+v", tr)
	}
	if end := mustTradeEvent(t, s).GetSequenceEnd(); end == nil || end.GetMarketSequence() != 3 {
		t.Fatalf("missing end: %+v", end)
	}
	hub.mu.Lock()
	count := len(hub.markets)
	hub.mu.Unlock()
	if count != 1 {
		t.Fatalf("unexpected hub workers %d", count)
	}
}
func TestHubTradesSkipNoTradeSequencesAndBoundOverflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub, _ := NewHub(ctx, &fakeSource{snapshotSeq: 1}, 50*time.Millisecond, time.Second, 1)
	defer hub.Shutdown(context.Background())
	sub, e := hub.SubscribeTrades(ctx, mkMarket())
	if e != nil {
		t.Fatal(e)
	}
	defer sub.Close()
	hub.mu.Lock()
	state := hub.markets[7]
	hub.mu.Unlock()
	for seq := uint64(2); seq < 1000; seq++ {
		r := mkTradeResult(seq)
		hub.broadcast(state, Feed{Result: &r})
	}
	r := mkTradeResult(1000, 0)
	hub.broadcast(state, Feed{Result: &r})
	select {
	case f := <-sub.Messages:
		f.Release()
		if f.Trades == nil || f.Trades.MarketSequence != 1000 || len(f.Trades.Trades) != 1 || f.Result != nil {
			t.Fatalf("trade-only feed: %+v", f)
		}
	case e := <-sub.Errors:
		t.Fatal(e)
	case <-time.After(time.Second):
		t.Fatal("trade update missing")
	}
	hub.mu.Lock()
	queued := hub.queuedBytes
	hub.mu.Unlock()
	if queued != 0 {
		t.Fatalf("queue credits leaked: %d", queued)
	}
	for seq := uint64(1001); seq <= 1003; seq++ {
		r := mkTradeResult(seq, 0)
		hub.broadcast(state, Feed{Result: &r})
	}
	select {
	case e := <-sub.Errors:
		if !errors.Is(e, ErrSlowConsumer) {
			t.Fatalf("overflow: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("must fail closed on trade queue overflow")
	}
}
func TestTradeRPCShutdownJoinsConnections(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1}
	hub, tr, client, cleanup := newTradeTestClient(t, src, 4, 1)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	s, e := client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT"}))
	if e != nil {
		t.Fatal(e)
	}
	if mustTradeEvent(t, s).GetStarted() == nil {
		t.Fatal("missing started")
	}
	closeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if e = tr.ShutdownAndWait(closeCtx); e != nil {
		t.Fatal(e)
	}
	if e = tr.ShutdownAndWait(closeCtx); e != nil {
		t.Fatal(e)
	}
	cancel()
	_ = s.Close()
	if e = hub.Shutdown(closeCtx); e != nil {
		t.Fatal(e)
	}
	if len(tr.capacity) != 0 {
		t.Fatal("capacity not reclaimed")
	}
	if e = tr.enter(); !errors.Is(e, ErrStreamShutdown) {
		t.Fatalf("post-shutdown enter: %v", e)
	}
}
func TestTradeHeartbeatDoesNotClaimNewTradeSequence(t *testing.T) {
	src := &fakeSource{snapshotSeq: 17}
	_, rpc, client, cleanup := newTradeTestClient(t, src, 8, 1)
	defer cleanup()
	rpc.heartbeatInterval = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, e := client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT"}))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if mustTradeEvent(t, s).GetStarted().GetHeadMarketSequence() != 17 {
		t.Fatal("wrong live baseline")
	}
	h := mustTradeEvent(t, s).GetHeartbeat()
	if h == nil || h.GetLastCompletedMarketSequence() != 17 {
		t.Fatalf("heartbeat: %+v", h)
	}
}
func TestValidateTradeBatchRejectsCorruptProvenance(t *testing.T) {
	m := mkMarket()
	for _, r := range [][]*md.TradeTick{
		{tradeTick(2, 2), tradeTick(2, 1)},
		{tradeTick(2, 0), tradeTick(2, 0)},
		{{MarketId: m.Symbol, SourceMarketId: m.SourceID, MatchSequence: 2, EventOrdinal: 0, TradeId: "07:2:0", MarketDefinitionVersion: 3}},
		{tradeTick(3, 0)},
	} {
		if err := validateTradeBatch(r, m, 2); !errors.Is(err, ErrSequenceGap) {
			t.Fatalf("accepted corrupt trade batch: %v", r)
		}
	}
}

func TestStreamTradesRequiresDeadlineMiddleware(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{snapshotSeq: 1}
	hub, e := NewHub(ctx, src, 50*time.Millisecond, time.Second, 4)
	if e != nil {
		t.Fatal(e)
	}
	defer hub.Shutdown(context.Background())
	rpc, e := NewTradeRPC(wsRegistry(t), src, hub, 2)
	if e != nil {
		t.Fatal(e)
	}
	path, handler := apiv1alpha1connect.NewMarketDataServiceHandler(&tradeTestService{trade: rpc})
	mux := http.NewServeMux()
	mux.Handle(path, handler) // deliberately omit write deadline wrapper
	server := httptest.NewServer(mux)
	defer server.Close()
	client := apiv1alpha1connect.NewMarketDataServiceClient(server.Client(), server.URL)
	reqCtx, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	s, e := client.StreamTrades(reqCtx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT"}))
	if e == nil {
		defer s.Close()
		if s.Receive() {
			t.Fatal("unsafe handler sent a stream frame without a write deadline")
		}
		e = s.Err()
	}
	if connect.CodeOf(e) != connect.CodeUnavailable {
		t.Fatalf("unexpected code %v", e)
	}
}

func TestStreamTradesReplayStormIsBounded(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1, committed: []Result{mkTradeResult(2, 0)}}
	_, rpc, client, cleanup := newTradeTestClient(t, src, 16, 2)
	defer cleanup()
	// Simulate a saturated replay pool. Live subscriptions remain independent.
	for i := 0; i < cap(rpc.replayCapacity); i++ {
		rpc.replayCapacity <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, e := client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT", ResumeAfter: completeTradeCursor(1)}))
	if e == nil {
		defer s.Close()
		if s.Receive() {
			t.Fatal("replay was admitted with no capacity")
		}
		e = s.Err()
	}
	if connect.CodeOf(e) != connect.CodeResourceExhausted {
		t.Fatalf("replay admission: %v", e)
	}
	for i := 0; i < cap(rpc.replayCapacity); i++ {
		<-rpc.replayCapacity
	}
	live, e := client.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: "BTC-USDT"}))
	if e != nil {
		t.Fatal(e)
	}
	defer live.Close()
	if start := mustTradeEvent(t, live).GetStarted(); start == nil || start.GetMode() != api.StreamTradesStarted_MODE_LIVE {
		t.Fatalf("live path blocked by replay storm: %+v", start)
	}
}
