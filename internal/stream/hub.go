package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"google.golang.org/protobuf/proto"
)

var ErrSlowConsumer = errors.New("stream: subscriber backlog exceeded; resync required")

// TradeBatch retains only public executions, not the book delta or candles.
// The hub has already verified contiguous matching sequences before fan-out.
type TradeBatch struct {
	MarketSequence uint64
	Trades         []*md.TradeTick
}

type Feed struct {
	Result *Result
	Soft   *Soft
	Trades *TradeBatch
	Ticker *md.Ticker // latest-state ticker lane; independent of periodic Soft
	// Every queued delivery reserves bytes until the receiver calls Release.
	// Call Release on receipt, BEFORE any potentially slow network write.
	reservation *feedReservation
}

type feedReservation struct {
	once  sync.Once
	hub   *Hub
	sub   *subscriber
	bytes int64
}

// releaseLocked must be called with Hub.mu held. sync.Once allows a receiver
// and a concurrent disconnect/drain to race without returning credits twice.
func (r *feedReservation) releaseLocked() {
	r.once.Do(func() {
		r.sub.queuedBytes -= r.bytes
		r.hub.queuedBytes -= r.bytes
	})
}

func (f Feed) Release() {
	if r := f.reservation; r != nil {
		r.hub.mu.Lock()
		r.releaseLocked()
		r.hub.mu.Unlock()
	}
}

type subscriptionKind uint8

const (
	subscriptionAll subscriptionKind = iota
	subscriptionBook
	subscriptionTrades
	subscriptionTicker
)

type subscriber struct {
	ch          chan Feed
	errs        chan error
	kind        subscriptionKind
	queuedBytes int64 // protected by Hub.mu
}

type Subscription struct {
	Messages    <-chan Feed
	Errors      <-chan error
	unsubscribe func()
	head        func() uint64
	once        sync.Once
}

// ObservedHead is the latest authoritative PostgreSQL head observed by the
// shared market worker. It is inexpensive to read and does not issue one PG
// query per client, even for thousands of subscribers.
func (s *Subscription) ObservedHead() uint64 {
	if s == nil || s.head == nil {
		return 0
	}
	return s.head()
}

func (s *Subscription) Close() {
	if s != nil {
		s.once.Do(s.unsubscribe)
	}
}

type marketHub struct {
	market         marketregistry.Market
	sequence       uint64 // written only by this market's run goroutine
	tickerSequence uint64 // last PG-consistent ticker published by this worker
	head           uint64 // protected by Hub.mu
	subs           map[*subscriber]struct{}
	cancel         context.CancelFunc
}

type Hub struct {
	source               Source
	parent               context.Context
	cancel               context.CancelFunc
	pollEvery, softEvery time.Duration
	queue                int
	policy               LagPolicy
	mu                   sync.Mutex
	markets              map[uint32]*marketHub
	queuedBytes          int64 // total outstanding queued protobuf bytes, under mu
	maxTotalQueuedBytes  int64
	workers              sync.WaitGroup // Add protected by mu, before close starts
	admissions           sync.WaitGroup // active subscriber registration queries
	closed               bool
	shutdownDone         chan struct{}
}

func NewHub(parent context.Context, src Source, pollEvery, softEvery time.Duration, queue int) (*Hub, error) {
	return NewHubWithLagPolicy(parent, src, pollEvery, softEvery, queue, DefaultLagPolicy())
}

func NewHubWithLagPolicy(parent context.Context, src Source, pollEvery, softEvery time.Duration, queue int, policy LagPolicy) (*Hub, error) {
	if parent == nil || src == nil || pollEvery < 50*time.Millisecond || softEvery < time.Second || queue < 1 || queue > 4096 || !policy.valid() {
		return nil, errors.New("stream hub: invalid source, intervals, queue, or stale-lag policy")
	}
	ctx, cancel := context.WithCancel(parent)
	return &Hub{source: src, parent: ctx, cancel: cancel, pollEvery: pollEvery, softEvery: softEvery,
		queue: queue, policy: policy, markets: map[uint32]*marketHub{}, maxTotalQueuedBytes: MaxTotalQueuedBytes, shutdownDone: make(chan struct{})}, nil
}

// Shutdown atomically refuses new subscriptions, notifies existing subscribers,
// cancels all market workers, and waits for them to exit. It is idempotent,
// and concurrent Shutdown calls share the same completion signal.
func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		h.cancel()
		for id, market := range h.markets {
			for sub := range market.subs {
				h.dropSubscriber(market, sub, ErrStreamShutdown)
			}
			market.cancel()
			delete(h.markets, id)
		}
		go func() {
			h.workers.Wait()
			h.admissions.Wait()
			close(h.shutdownDone)
		}()
	}
	done := h.shutdownDone
	h.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe registers BEFORE the WebSocket takes its PostgreSQL snapshot.
// Any later committed event is buffered or is still in the durable journal.
func (h *Hub) Subscribe(ctx context.Context, m marketregistry.Market) (*Subscription, error) {
	return h.admit(ctx, m, subscriptionAll)
}

// SubscribeBook shares the market worker, but its subscribers retain ONLY book
// delta bytes; trades/candles and periodic soft updates are not queued.
func (h *Hub) SubscribeBook(ctx context.Context, m marketregistry.Market) (*Subscription, error) {
	return h.admit(ctx, m, subscriptionBook)
}

// SubscribeTicker shares the market worker but uses a SINGLE pending latest
// ticker slot. Superseded values may be replaced, never reordered. This lane
// never queues order-book/trade results or the periodic WebSocket Soft feed.
func (h *Hub) SubscribeTicker(ctx context.Context, m marketregistry.Market) (*Subscription, error) {
	if _, ok := h.source.(TickerSource); !ok {
		return nil, errors.New("stream: ticker source unavailable")
	}
	return h.admit(ctx, m, subscriptionTicker)
}

// SubscribeTrades does not queue non-trading matching results, candles,
// book deltas, or ticker refreshes. Hub's journal worker still verifies all
// sequences before publishing any trade batch.
func (h *Hub) SubscribeTrades(ctx context.Context, m marketregistry.Market) (*Subscription, error) {
	return h.admit(ctx, m, subscriptionTrades)
}

func (h *Hub) admit(ctx context.Context, m marketregistry.Market, kind subscriptionKind) (*Subscription, error) {
	if ctx == nil {
		return nil, errors.New("stream: missing request context")
	}
	h.mu.Lock()
	if h.closed || h.parent.Err() != nil {
		h.mu.Unlock()
		return nil, ErrStreamShutdown
	}
	h.admissions.Add(1)
	h.mu.Unlock()
	defer h.admissions.Done()
	// Shutdown must cancel an in-flight registration's database read, even
	// before a subscriber has been inserted into the market hub.
	readCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(h.parent, cancel)
	defer func() { stop(); cancel() }()
	return h.subscribe(readCtx, m, kind)
}

func (h *Hub) subscribe(ctx context.Context, m marketregistry.Market, kind subscriptionKind) (*Subscription, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h.mu.Lock()
		if h.closed || h.parent.Err() != nil {
			h.mu.Unlock()
			return nil, ErrStreamShutdown
		}
		state := h.markets[m.SourceID]
		h.mu.Unlock()
		if state == nil {
			// Query outside the lock; the second check below prevents resurrection
			// of a closed market or hub during a slow PostgreSQL snapshot.
			snap, err := h.source.Snapshot(ctx, m)
			if err != nil {
				return nil, err
			}
			if snap == nil || proto.Size(snap) > MaxSnapshotBytes {
				return nil, ErrPayloadTooLarge
			}
			h.mu.Lock()
			if h.closed || h.parent.Err() != nil {
				h.mu.Unlock()
				return nil, ErrStreamShutdown
			}
			state = h.markets[m.SourceID]
			if state == nil {
				runCtx, cancel := context.WithCancel(h.parent)
				state = &marketHub{market: m, sequence: snap.GetMarketSequence(), head: snap.GetMarketSequence(), subs: map[*subscriber]struct{}{}, cancel: cancel}
				h.markets[m.SourceID] = state
				h.workers.Add(1)
				go h.run(runCtx, state)
			}
			h.mu.Unlock()
		}
		queueSize := h.queue
		if kind == subscriptionTicker {
			queueSize = 1 // latest state replaces stale buffered state
		}
		sub := &subscriber{ch: make(chan Feed, queueSize), errs: make(chan error, 1), kind: kind}
		h.mu.Lock()
		if h.closed || h.parent.Err() != nil {
			h.mu.Unlock()
			return nil, ErrStreamShutdown
		}
		if h.markets[m.SourceID] != state {
			h.mu.Unlock()
			continue // market worker ended before registration
		}
		state.subs[sub] = struct{}{}
		h.mu.Unlock()
		return &Subscription{Messages: sub.ch, Errors: sub.errs,
			head: func() uint64 {
				h.mu.Lock()
				defer h.mu.Unlock()
				return state.head
			},
			unsubscribe: func() {
				h.mu.Lock()
				h.drainQueued(sub)
				delete(state.subs, sub)
				if len(state.subs) == 0 && h.markets[m.SourceID] == state {
					delete(h.markets, m.SourceID)
					state.cancel()
				}
				h.mu.Unlock()
			}}, nil
	}
}

func (h *Hub) broadcast(state *marketHub, feed Feed) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.markets[state.market.SourceID] != state {
		return
	}
	for s := range state.subs {
		outgoing := feed
		var bytes int64
		if s.kind == subscriptionTicker {
			if feed.Ticker == nil {
				continue
			}
			outgoing = Feed{Ticker: feed.Ticker}
			bytes = int64(proto.Size(feed.Ticker))
		} else if s.kind == subscriptionBook {
			if feed.Result == nil {
				continue
			}
			if feed.Result.Delta == nil {
				h.dropSubscriber(state, s, ErrSequenceGap)
				continue
			}
			outgoing = Feed{Result: &Result{Delta: feed.Result.Delta}}
			bytes = int64(proto.Size(feed.Result.Delta))
		} else if s.kind == subscriptionTrades {
			if feed.Result == nil { // no ticker/soft updates on a trade stream
				continue
			}
			if feed.Result.Delta == nil {
				h.dropSubscriber(state, s, ErrSequenceGap)
				continue
			}
			if len(feed.Result.Trades) == 0 { // no per-sequence traffic for empty results
				continue
			}
			for _, trade := range feed.Result.Trades {
				if trade == nil {
					bytes = MaxFeedBytes + 1
					break
				}
				bytes += int64(proto.Size(trade))
			}
			outgoing = Feed{Trades: &TradeBatch{MarketSequence: feed.Result.Delta.GetMarketSequence(), Trades: feed.Result.Trades}}
		} else {
			if feed.Ticker != nil { // dedicated Connect ticker, not WebSocket soft
				continue
			}
			var err error
			if feed.Result != nil {
				bytes, err = resultBytes(*feed.Result)
			} else if feed.Soft != nil {
				if feed.Soft.Ticker != nil {
					bytes += int64(proto.Size(feed.Soft.Ticker))
				}
				if feed.Soft.Stats != nil {
					bytes += int64(proto.Size(feed.Soft.Stats))
				}
			}
			if err != nil {
				h.dropSubscriber(state, s, err)
				continue
			}
		}
		if bytes > MaxFeedBytes {
			h.dropSubscriber(state, s, ErrPayloadTooLarge)
			continue
		}
		if s.kind == subscriptionTicker {
			// Only the pending (not currently writing) value is replaceable.
			// Release its byte credits BEFORE reserving the newer value.
			select {
			case old := <-s.ch:
				if old.reservation != nil {
					old.reservation.releaseLocked()
				}
			default:
			}
		}
		if s.queuedBytes+bytes > MaxSubscriberBytes || h.queuedBytes+bytes > h.maxTotalQueuedBytes {
			h.dropSubscriber(state, s, ErrSlowConsumer)
			continue
		}
		outgoing.reservation = &feedReservation{hub: h, sub: s, bytes: bytes}
		select {
		case s.ch <- outgoing:
			s.queuedBytes += bytes
			h.queuedBytes += bytes
		default:
			h.dropSubscriber(state, s, ErrSlowConsumer)
		}
	}
	if len(state.subs) == 0 {
		delete(h.markets, state.market.SourceID)
		state.cancel()
	}
}

// Flush entries still in the bounded channel before releasing a subscriber.
// An already dequeued entry remains accounted until its receiver calls Release.
// Both paths serialize on the same mutex and reservation sync.Once.
func (h *Hub) drainQueued(sub *subscriber) {
	for {
		select {
		case queued := <-sub.ch:
			if queued.reservation != nil {
				queued.reservation.releaseLocked()
			}
		default:
			return
		}
	}
}

// Called only while holding h.mu.
func (h *Hub) dropSubscriber(state *marketHub, sub *subscriber, err error) {
	select {
	case sub.errs <- err:
	default:
	}
	h.drainQueued(sub)
	delete(state.subs, sub)
}

// A ticker read failure is isolated to ticker consumers: it must not
// invalidate the book/trade journal subscribers sharing the market worker.
func (h *Hub) failTicker(state *marketHub, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.markets[state.market.SourceID] != state {
		return
	}
	for s := range state.subs {
		if s.kind == subscriptionTicker {
			h.dropSubscriber(state, s, err)
		}
	}
	if len(state.subs) == 0 {
		delete(h.markets, state.market.SourceID)
		state.cancel()
	}
}

// refreshTicker is driven by the FAST journal poll cadence, not softEvery.
// Each market issues at most one coherent PG ticker read per poll regardless
// of its subscriber count. A ticker is a replaceable latest-state observation,
// so gaps are expected, but regression/corruption terminates ticker streams.
func (h *Hub) refreshTicker(ctx context.Context, state *marketHub) {
	h.mu.Lock()
	needed := false
	if h.markets[state.market.SourceID] == state {
		for s := range state.subs {
			if s.kind == subscriptionTicker {
				needed = true
				break
			}
		}
	}
	h.mu.Unlock()
	if !needed {
		return
	}
	reader := h.source.(TickerSource) // SubscribeTicker verified capability
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	ticker, err := reader.LatestTicker(readCtx, state.market)
	cancel()
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		err = validateStreamTicker(ticker, state.market)
	}
	if err != nil {
		h.failTicker(state, err)
		return
	}
	seq := ticker.GetMarketSequence()
	if seq < state.tickerSequence {
		h.failTicker(state, ErrSequenceGap)
		return
	}
	if seq == state.tickerSequence {
		return
	}
	state.tickerSequence = seq
	h.broadcast(state, Feed{Ticker: ticker})
}

func (h *Hub) fail(state *marketHub, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.markets[state.market.SourceID] != state {
		return
	}
	for sub := range state.subs {
		h.dropSubscriber(state, sub, err)
	}
	delete(h.markets, state.market.SourceID)
	state.cancel()
}

// checkLag uses the latest DURABLE head, not wall-clock time since the last
// trade. No events => head == cursor => not stale. Lag age starts on first
// observation of an outstanding head and resets after catching up.
func checkLag(cursor, head uint64, now time.Time, behindSince *time.Time, policy LagPolicy) error {
	if head < cursor {
		return ErrSequenceGap
	}
	if head == cursor {
		*behindSince = time.Time{}
		return nil
	}
	if behindSince.IsZero() {
		*behindSince = now
	}
	if head-cursor > policy.MaxHeadSequences || now.Sub(*behindSince) >= policy.MaxBehind {
		return ErrStreamStale
	}
	return nil
}

// boundedResults shrinks only an over-budget multi-sequence query. It never
// omits a matching sequence: the next query continues from the last published
// cursor. If even one complete sequence exceeds the budget, fail closed.
func boundedResults(ctx context.Context, src Source, m marketregistry.Market, after, through uint64, limit int) ([]Result, int, error) {
	for {
		results, err := src.Results(ctx, m, after, through, limit)
		if err == nil || !errors.Is(err, ErrBatchTooLarge) || limit == 1 {
			return results, limit, err
		}
		limit = (limit + 1) / 2
	}
}

func (h *Hub) run(ctx context.Context, state *marketHub) {
	defer h.workers.Done()
	poll := time.NewTicker(h.pollEvery)
	defer poll.Stop()
	soft := time.NewTicker(h.softEvery)
	defer soft.Stop()
	var behindSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			// Ticker latest-state updates do not wait for journal catch-up or
			// the slower periodic WebSocket soft refresh. Read once per market.
			h.refreshTicker(ctx, state)
			if ctx.Err() != nil {
				return
			}
			// Drain a limited number of full batches per tick, preserving the
			// exact sequence. No backpressure policy ever skips durable events.
			for batch := 0; batch < h.policy.MaxPollBatches; batch++ {
				if ctx.Err() != nil {
					return
				}
				queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				results, effectiveLimit, err := boundedResults(queryCtx, h.source, state.market, state.sequence, 0, 256)
				cancel()
				if err != nil {
					if ctx.Err() == nil {
						h.fail(state, fmt.Errorf("durable stream poll: %w", err))
					}
					return
				}
				for i := range results {
					if ctx.Err() != nil {
						return
					}
					next := results[i].Delta.GetMarketSequence()
					if next <= state.sequence || next-state.sequence != 1 {
						h.fail(state, ErrSequenceGap)
						return
					}
					if _, err := resultBytes(results[i]); err != nil {
						h.fail(state, err)
						return
					}
					state.sequence = next
					r := results[i]
					h.broadcast(state, Feed{Result: &r})
				}
				if len(results) < effectiveLimit {
					break
				}
			}
			if ctx.Err() != nil {
				return
			}
			// HEAD is read after draining so the comparison cannot mistake
			// the latest published event for an unprocessed event.
			headCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			head, err := h.source.Head(headCtx, state.market)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					h.fail(state, fmt.Errorf("durable stream head: %w", err))
				}
				return
			}
			h.mu.Lock()
			state.head = head
			h.mu.Unlock()
			if err := checkLag(state.sequence, head, time.Now(), &behindSince, h.policy); err != nil {
				h.fail(state, err)
				return
			}
		case <-soft.C:
			queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			result, err := h.source.Current(queryCtx, state.market, time.Now().UTC())
			cancel()
			if ctx.Err() != nil {
				return
			}
			if err == nil && result != nil {
				h.broadcast(state, Feed{Soft: result})
			}
		}
	}
}
