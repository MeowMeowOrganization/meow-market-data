package stream

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func addUnknownPayload(message proto.Message, size int) {
	blob := protowire.AppendTag(nil, 500, protowire.BytesType)
	blob = protowire.AppendBytes(blob, make([]byte, size))
	message.ProtoReflect().SetUnknown(blob)
}

func TestExactQueuedByteBudgetAndRelease(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	source := &fakeSource{snapshotSeq: 1}
	hub, err := NewHub(ctx, source, 50*time.Millisecond, time.Second, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Shutdown(context.Background())
	sub, err := hub.SubscribeBook(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	hub.mu.Lock()
	state := hub.markets[mkMarket().SourceID]
	hub.mu.Unlock()
	for i := uint64(2); i <= 5; i++ {
		r := mkResult(i)
		addUnknownPayload(r.Delta, MaxSubscriberBytes/4-64)
		hub.broadcast(state, Feed{Result: &r})
	}
	var queued []Feed
	for i := 0; i < 4; i++ {
		select {
		case f := <-sub.Messages:
			queued = append(queued, f)
		case <-time.After(time.Second):
			t.Fatal("missing queued event")
		}
	}
	for _, f := range queued {
		f.Release()
		f.Release()
	}
	hub.mu.Lock()
	bytes := state.subsValueQueuedBytes(sub)
	hub.mu.Unlock()
	if bytes != 0 {
		t.Fatalf("byte reservations not released: %d", bytes)
	}
	hub.mu.Lock()
	global := hub.queuedBytes
	hub.mu.Unlock()
	if global != 0 {
		t.Fatalf("global byte credits not returned: %d", global)
	}
	// More messages can now be enqueued without exhausting the queue budget.
	r := mkResult(6)
	addUnknownPayload(r.Delta, 700<<10)
	hub.broadcast(state, Feed{Result: &r})
	select {
	case f := <-sub.Messages:
		f.Release()
	case e := <-sub.Errors:
		t.Fatalf("unexpected overflow: %v", e)
	case <-time.After(time.Second):
		t.Fatal("missing message after release")
	}
}

// Helper reads the same counter as the enqueue path, under Hub.mu.
func (m *marketHub) subsValueQueuedBytes(s *Subscription) int64 {
	for sub := range m.subs {
		if sub.ch == s.Messages {
			return sub.queuedBytes
		}
	}
	return -1
}

func TestQueuedByteBudgetExhaustionNeverDropsSilently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub, _ := NewHub(ctx, &fakeSource{snapshotSeq: 1}, 50*time.Millisecond, time.Second, 32)
	defer hub.Shutdown(context.Background())
	sub, err := hub.SubscribeBook(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	hub.mu.Lock()
	state := hub.markets[7]
	hub.mu.Unlock()
	for i := uint64(2); i <= 8; i++ {
		r := mkResult(i)
		addUnknownPayload(r.Delta, 700<<10)
		hub.broadcast(state, Feed{Result: &r})
	}
	select {
	case e := <-sub.Errors:
		if !errors.Is(e, ErrSlowConsumer) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("byte budget did not disconnect client")
	}
}

func TestOversizedPayloadFailsClosed(t *testing.T) {
	r := mkResult(2)
	addUnknownPayload(r.Delta, MaxFeedBytes)
	if _, err := resultBytes(r); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversize result: %v", err)
	}
	policy := DefaultLagPolicy()
	if !policy.valid() {
		t.Fatal("invalid default policy")
	}
	if (LagPolicy{MaxHeadSequences: 0, MaxBehind: time.Second, MaxPollBatches: 1}).valid() {
		t.Fatal("zero lag accepted")
	}
}

func TestLagPolicyFailsOnSequenceOrTimeButNotIdle(t *testing.T) {
	p := LagPolicy{MaxHeadSequences: 3, MaxBehind: time.Second, MaxPollBatches: 1}
	now := time.Unix(100, 0)
	var since time.Time
	if err := checkLag(10, 10, now, &since, p); err != nil || !since.IsZero() {
		t.Fatalf("idle: %v", err)
	}
	if err := checkLag(10, 13, now, &since, p); err != nil {
		t.Fatal(err)
	}
	if err := checkLag(11, 13, now.Add(900*time.Millisecond), &since, p); err != nil {
		t.Fatal(err)
	}
	if err := checkLag(11, 13, now.Add(time.Second), &since, p); !errors.Is(err, ErrStreamStale) {
		t.Fatalf("age: %v", err)
	}
	if err := checkLag(15, 15, now.Add(time.Second), &since, p); err != nil || !since.IsZero() {
		t.Fatalf("catchup: %v", err)
	}
	if err := checkLag(1, 5, now, &since, p); !errors.Is(err, ErrStreamStale) {
		t.Fatalf("sequence: %v", err)
	}
	if err := checkLag(8, 7, now, &since, p); !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("head regression: %v", err)
	}
}

type stalledHeadSource struct {
	*fakeSource
	head uint64
}

func (s *stalledHeadSource) Head(context.Context, marketregistry.Market) (uint64, error) {
	return s.head, nil
}

func TestHubDisconnectsOnDurableHeadLag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &stalledHeadSource{fakeSource: &fakeSource{snapshotSeq: 1}, head: 100}
	hub, err := NewHubWithLagPolicy(ctx, src, 50*time.Millisecond, time.Second, 16,
		LagPolicy{MaxHeadSequences: 3, MaxBehind: time.Second, MaxPollBatches: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Shutdown(context.Background())
	sub, err := hub.SubscribeBook(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	select {
	case e := <-sub.Errors:
		if !errors.Is(e, ErrStreamStale) {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stale hub remained live")
	}
}

type blockedResultSource struct {
	*fakeSource
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (s *blockedResultSource) Results(_ context.Context, _ marketregistry.Market, _, _ uint64, _ int) ([]Result, error) {
	s.once.Do(func() { close(s.started) })
	<-s.release // deliberately ignores cancellation to test shutdown join
	return nil, nil
}

func TestHubShutdownJoinsAllWorkersAndRejectsNewSubscribers(t *testing.T) {
	src := &blockedResultSource{fakeSource: &fakeSource{snapshotSeq: 1}, started: make(chan struct{}), release: make(chan struct{})}
	hub, err := NewHub(context.Background(), src, 50*time.Millisecond, time.Second, 8)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := hub.SubscribeBook(context.Background(), mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	select {
	case <-src.started:
	case <-time.After(time.Second):
		t.Fatal("poll never started")
	}
	short, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := hub.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("must wait on running worker, got %v", err)
	}
	if _, err := hub.SubscribeBook(context.Background(), mkMarket()); !errors.Is(err, ErrStreamShutdown) {
		t.Fatalf("new subscription after shutdown: %v", err)
	}
	close(src.release)
	wait, finish := context.WithTimeout(context.Background(), time.Second)
	defer finish()
	if err := hub.Shutdown(wait); err != nil {
		t.Fatal(err)
	}
	if err := hub.Shutdown(wait); err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	count := len(hub.markets)
	hub.mu.Unlock()
	if count != 0 {
		t.Fatalf("orphan markets: %d", count)
	}
}

type gatedSnapshotSource struct {
	*fakeSource
	entered chan struct{}
	release chan struct{}
}

func (s *gatedSnapshotSource) Snapshot(ctx context.Context, market marketregistry.Market) (*md.BookSnapshot, error) {
	close(s.entered)
	<-s.release
	return s.fakeSource.Snapshot(ctx, market)
}
func TestShutdownCannotResurrectDelayedSubscription(t *testing.T) {
	src := &gatedSnapshotSource{fakeSource: &fakeSource{snapshotSeq: 1}, entered: make(chan struct{}), release: make(chan struct{})}
	hub, _ := NewHub(context.Background(), src, 50*time.Millisecond, time.Second, 8)
	result := make(chan error, 1)
	go func() { _, err := hub.SubscribeBook(context.Background(), mkMarket()); result <- err }()
	select {
	case <-src.entered:
	case <-time.After(time.Second):
		t.Fatal("subscribe not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	short, cancelShort := context.WithTimeout(context.Background(), 30*time.Millisecond)
	if err := hub.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown must join pending registration: %v", err)
	}
	cancelShort()
	close(src.release)
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrStreamShutdown) {
			t.Fatalf("resurrected: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("subscription stuck after shutdown")
	}
}

func TestConnectShutdownWaitsAndBlocksAdmission(t *testing.T) {
	src := &fakeSource{snapshotSeq: 1}
	_, b, client, cleanup := newBookTestClient(t, src, 8, 1)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	s, err := client.StreamOrderBook(ctx, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: "BTC-USDT", Depth: 20}))
	if err != nil {
		t.Fatal(err)
	}
	if mustReceiveBook(t, s).GetSnapshot() == nil {
		t.Fatal("missing snapshot")
	}
	deadline, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := b.ShutdownAndWait(deadline); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	active := b.active
	b.mu.Unlock()
	if active != 0 {
		t.Fatalf("active RPC after shutdown: %d", active)
	}
	if err := b.ShutdownAndWait(deadline); err != nil {
		t.Fatal(err)
	}
	if err := b.enter(); !errors.Is(err, ErrStreamShutdown) {
		t.Fatalf("accepted after shutdown: %v", err)
	}
	cancel()
	_ = s.Close()
}

func TestWebSocketShutdownClosesHijackedConnections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{snapshotSeq: 1}
	hub, _ := NewHub(ctx, src, 50*time.Millisecond, time.Second, 8)
	defer hub.Shutdown(context.Background())
	handler, _ := NewHandler(wsRegistry(t), src, hub, nil, 2)
	handler.Shutdown = ctx.Done()
	server := httptest.NewServer(handler)
	defer server.Close()
	path := "ws" + strings.TrimPrefix(server.URL, "http") + "?market_id=BTC-USDT&channels=book"
	ws, _, err := websocket.DefaultDialer.Dial(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	_ = readFrame(t, ws) // initial handshake
	deadline, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := handler.ShutdownAndWait(deadline); err != nil {
		t.Fatal(err)
	}
	if err := handler.ShutdownAndWait(deadline); err != nil {
		t.Fatal(err)
	}
	handler.mu.Lock()
	active := len(handler.active)
	handler.mu.Unlock()
	if active != 0 {
		t.Fatalf("orphan WebSocket handler: %d", active)
	}
	_, _, err = websocket.DefaultDialer.Dial(path, nil)
	if err == nil {
		t.Fatal("WS accepted after shutdown")
	}
}

func TestWebSocketFrameBudgetBeforeWrite(t *testing.T) {
	h := &Handler{}
	if err := h.writeControl(nil, map[string]any{"big": strings.Repeat("x", MaxWebSocketBytes)}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("control frame: %v", err)
	}
	if err := h.writeUpdate(nil, &md.MarketDataUpdate{Update: &md.MarketDataUpdate_Ticker{Ticker: &md.Ticker{MarketId: strings.Repeat("x", MaxWebSocketBytes)}}}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("data frame: %v", err)
	}
}

// The query-size budget is not a sequence-loss policy: retry with smaller
// complete batches before returning a failure to the transport.
type adaptiveSource struct {
	*fakeSource
	attempts []int
}

func (s *adaptiveSource) Results(_ context.Context, _ marketregistry.Market, after, _ uint64, limit int) ([]Result, error) {
	s.attempts = append(s.attempts, limit)
	if limit > 4 {
		return nil, ErrBatchTooLarge
	}
	var results []Result
	for n := uint64(1); n <= uint64(limit); n++ {
		results = append(results, mkResult(after+n))
	}
	return results, nil
}
func TestBoundedResultsAdaptWithoutSkipping(t *testing.T) {
	src := &adaptiveSource{fakeSource: &fakeSource{snapshotSeq: 1}}
	got, limit, err := boundedResults(context.Background(), src, mkMarket(), 10, 0, 32)
	if err != nil {
		t.Fatal(err)
	}
	if limit != 4 || len(got) != 4 || got[0].Delta.GetMarketSequence() != 11 || got[3].Delta.GetMarketSequence() != 14 {
		t.Fatalf("did not return contiguous bounded prefix: limit=%d results=%v", limit, got)
	}
	if len(src.attempts) != 4 || src.attempts[0] != 32 || src.attempts[3] != 4 {
		t.Fatalf("unexpected batch retries %v", src.attempts)
	}
}

type alwaysTooLargeSource struct{ *fakeSource }

func (s *alwaysTooLargeSource) Results(context.Context, marketregistry.Market, uint64, uint64, int) ([]Result, error) {
	return nil, ErrBatchTooLarge
}
func TestBoundedResultsSingleOversizedSequenceIsTerminal(t *testing.T) {
	src := &alwaysTooLargeSource{fakeSource: &fakeSource{snapshotSeq: 1}}
	_, limit, err := boundedResults(context.Background(), src, mkMarket(), 1, 0, 256)
	if limit != 1 || !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("limit=%d err=%v", limit, err)
	}
}

// Large snapshots must be rejected before the first event is sent to the client.
type hugeSnapshotSource struct{ *fakeSource }

func (s *hugeSnapshotSource) Snapshot(ctx context.Context, m marketregistry.Market) (*md.BookSnapshot, error) {
	snap, err := s.fakeSource.Snapshot(ctx, m)
	if err == nil {
		addUnknownPayload(snap, MaxSnapshotBytes)
	}
	return snap, err
}
func TestConnectOversizedSnapshotFailsClosed(t *testing.T) {
	src := &hugeSnapshotSource{fakeSource: &fakeSource{snapshotSeq: 1}}
	_, _, client, cleanup := newBookTestClient(t, src, 8, 1)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := client.StreamOrderBook(ctx, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: "BTC-USDT", Depth: 20}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Fatal(err)
		}
		return
	}
	defer s.Close()
	if s.Receive() {
		t.Fatal("oversized snapshot was delivered")
	}
	if connect.CodeOf(s.Err()) != connect.CodeResourceExhausted {
		t.Fatalf("bad code %v", s.Err())
	}
}

func TestConnectDurableHeadLagRequiresFreshSnapshot(t *testing.T) {
	src := &stalledHeadSource{fakeSource: &fakeSource{snapshotSeq: 1}, head: 10000}
	_, _, client, cleanup := newBookTestClient(t, src, 8, 1)
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
		t.Fatal("stale stream continued")
	}
	if connect.CodeOf(s.Err()) != connect.CodeUnavailable {
		t.Fatalf("unexpected error: %v", s.Err())
	}
}

func TestWebSocketFallsBackToSnapshotWhenReconnectLagExceedsPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{snapshotSeq: 5000}
	hub, _ := NewHubWithLagPolicy(ctx, src, 50*time.Millisecond, time.Second, 8,
		LagPolicy{MaxHeadSequences: 50, MaxBehind: time.Second, MaxPollBatches: 1})
	defer hub.Shutdown(context.Background())
	handler, _ := NewHandler(wsRegistry(t), src, hub, nil, 2)
	handler.Shutdown = ctx.Done()
	server := httptest.NewServer(handler)
	defer server.Close()
	path := "ws" + strings.TrimPrefix(server.URL, "http") + "?market_id=BTC-USDT&after_sequence=100&market_definition_version=3"
	ws, _, err := websocket.DefaultDialer.Dial(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if ready := readFrame(t, ws); ready["mode"] != "snapshot" {
		t.Fatalf("stale cursor was replayed instead of reset: %v", ready)
	}
	if update := readFrame(t, ws); update["type"] != "update" {
		t.Fatalf("snapshot missing: %v", update)
	}
}

func TestHubGlobalByteBudgetAndDisconnectReclaim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub, err := NewHub(ctx, &fakeSource{snapshotSeq: 1}, 50*time.Millisecond, time.Second, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Shutdown(context.Background())
	// Force a small global ceiling to verify independent subscriber queues
	// cannot collectively exceed the process-wide shared budget.
	hub.mu.Lock()
	hub.maxTotalQueuedBytes = 1 << 20
	hub.mu.Unlock()
	a, err := hub.SubscribeBook(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := hub.SubscribeBook(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	hub.mu.Lock()
	state := hub.markets[7]
	hub.mu.Unlock()
	r := mkResult(2)
	addUnknownPayload(r.Delta, 700<<10)
	hub.broadcast(state, Feed{Result: &r})
	hub.mu.Lock()
	global := hub.queuedBytes
	remaining := len(state.subs)
	hub.mu.Unlock()
	if remaining != 1 || global <= 0 || global > 1<<20 {
		t.Fatalf("global cap not enforced: remaining=%d bytes=%d", remaining, global)
	}
	var surviving Feed
	select {
	case surviving = <-a.Messages:
	case surviving = <-b.Messages:
	case <-time.After(time.Second):
		t.Fatal("no surviving subscriber")
	}
	surviving.Release()
	hub.mu.Lock()
	global = hub.queuedBytes
	hub.mu.Unlock()
	if global != 0 {
		t.Fatalf("global reservation leaked: %d", global)
	}
}
