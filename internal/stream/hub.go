package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
)

var ErrSlowConsumer = errors.New("stream: subscriber backlog exceeded; resync required")

type Feed struct {
	Result *Result
	Soft   *Soft
}
type subscriber struct {
	ch       chan Feed
	errs     chan error
	bookOnly bool
}

type Subscription struct {
	Messages    <-chan Feed
	Errors      <-chan error
	unsubscribe func()
	once        sync.Once
}

func (s *Subscription) Close() {
	if s != nil {
		s.once.Do(s.unsubscribe)
	}
}

type marketHub struct {
	market   marketregistry.Market
	sequence uint64
	subs     map[*subscriber]struct{}
	cancel   context.CancelFunc
}

type Hub struct {
	source               Source
	parent               context.Context
	pollEvery, softEvery time.Duration
	queue                int
	mu                   sync.Mutex
	markets              map[uint32]*marketHub
}

func NewHub(parent context.Context, src Source, pollEvery, softEvery time.Duration, queue int) (*Hub, error) {
	if parent == nil || src == nil || pollEvery < 50*time.Millisecond || softEvery < time.Second || queue < 1 || queue > 4096 {
		return nil, errors.New("stream hub: invalid source, poll, soft interval or queue")
	}
	return &Hub{source: src, parent: parent, pollEvery: pollEvery, softEvery: softEvery, queue: queue, markets: map[uint32]*marketHub{}}, nil
}

// Subscribe registers BEFORE the WebSocket takes its PostgreSQL snapshot.
// Any later committed event is buffered or is still in the durable journal.
func (h *Hub) Subscribe(ctx context.Context, m marketregistry.Market) (*Subscription, error) {
	return h.subscribe(ctx, m, false)
}

// SubscribeBook shares the market worker but queues ONLY book deltas, not
// trades/candles or periodic soft updates. This avoids charging an L2 client
// for unrelated feeds, and prevents large trade batches being kept alive by
// each book-only subscriber queue.
func (h *Hub) SubscribeBook(ctx context.Context, m marketregistry.Market) (*Subscription, error) {
	return h.subscribe(ctx, m, true)
}

func (h *Hub) subscribe(ctx context.Context, m marketregistry.Market, bookOnly bool) (*Subscription, error) {
	h.mu.Lock()
	state := h.markets[m.SourceID]
	h.mu.Unlock()
	if state == nil {
		snap, err := h.source.Snapshot(ctx, m)
		if err != nil {
			return nil, err
		}
		h.mu.Lock()
		state = h.markets[m.SourceID]
		if state == nil {
			runCtx, cancel := context.WithCancel(h.parent)
			state = &marketHub{market: m, sequence: snap.GetMarketSequence(), subs: map[*subscriber]struct{}{}, cancel: cancel}
			h.markets[m.SourceID] = state
			go h.run(runCtx, state)
		}
		h.mu.Unlock()
	}
	sub := &subscriber{ch: make(chan Feed, h.queue), errs: make(chan error, 1), bookOnly: bookOnly}
	h.mu.Lock()
	// A prior state could have been stopped between snapshot and registration.
	if h.markets[m.SourceID] != state {
		h.mu.Unlock()
		return h.subscribe(ctx, m, bookOnly)
	}
	state.subs[sub] = struct{}{}
	h.mu.Unlock()
	return &Subscription{Messages: sub.ch, Errors: sub.errs, unsubscribe: func() {
		h.mu.Lock()
		delete(state.subs, sub)
		if len(state.subs) == 0 && h.markets[m.SourceID] == state {
			delete(h.markets, m.SourceID)
			state.cancel()
		}
		h.mu.Unlock()
	}}, nil
}
func (h *Hub) broadcast(state *marketHub, feed Feed) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.markets[state.market.SourceID] != state {
		return
	}
	for s := range state.subs {
		outgoing := feed
		if s.bookOnly {
			if feed.Result == nil { // no ticker/stats polling noise
				continue
			}
			if feed.Result.Delta == nil {
				select {
				case s.errs <- ErrSequenceGap:
				default:
				}
				delete(state.subs, s)
				continue
			}
			outgoing = Feed{Result: &Result{Delta: feed.Result.Delta}}
		}
		select {
		case s.ch <- outgoing:
		default:
			select {
			case s.errs <- ErrSlowConsumer:
			default:
			}
			delete(state.subs, s)
		}
	}
	if len(state.subs) == 0 {
		delete(h.markets, state.market.SourceID)
		state.cancel()
	}
}
func (h *Hub) fail(state *marketHub, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.markets[state.market.SourceID] != state {
		return
	}
	for sub := range state.subs {
		select {
		case sub.errs <- err:
		default:
		}
	}
	delete(h.markets, state.market.SourceID)
	state.cancel()
}
func (h *Hub) run(ctx context.Context, state *marketHub) {
	poll := time.NewTicker(h.pollEvery)
	defer poll.Stop()
	soft := time.NewTicker(h.softEvery)
	defer soft.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			// At most a bounded batch per tick. In high volume, each successive
			// tick resumes at the exact next sequence without skipping history.
			queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			results, err := h.source.Results(queryCtx, state.market, state.sequence, 0, 256)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					h.fail(state, fmt.Errorf("durable stream poll: %w", err))
				}
				return
			}
			for i := range results {
				next := results[i].Delta.GetMarketSequence()
				if next <= state.sequence || next-state.sequence != 1 {
					h.fail(state, ErrSequenceGap)
					return
				}
				state.sequence = next // only this goroutine changes sequence
				r := results[i]
				h.broadcast(state, Feed{Result: &r})
			}
		case <-soft.C:
			queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			result, err := h.source.Current(queryCtx, state.market, time.Now().UTC())
			cancel()
			if err == nil && result != nil {
				h.broadcast(state, Feed{Soft: result})
			}
			// A temporary Redis/PG failure for soft stats never moves the
			// durable sequence cursor and does not close healthy book streams.
		}
	}
}
