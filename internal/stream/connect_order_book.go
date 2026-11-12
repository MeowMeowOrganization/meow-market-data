package stream

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"google.golang.org/protobuf/proto"
)

const (
	bookInitialReadTimeout = 5 * time.Second
	bookSendTimeout        = 5 * time.Second
	bookHeartbeatInterval  = 20 * time.Second
)

// bookResponseControllerKey is set by WithStreamWriteDeadlines, which must
// wrap the generated Connect handler. Deadline enforcement is not optional:
// otherwise a peer that stops reading can block a stream.Send indefinitely.
type bookResponseControllerKey struct{}

// WithStreamWriteDeadlines exposes the HTTP response controller to the RPC
// method without changing Connect's generated interfaces. Do not wrap the
// ResponseWriter with an unwrappable custom writer: ResponseController needs
// the underlying HTTP/1 or HTTP/2 transport's SetWriteDeadline support.
func WithStreamWriteDeadlines(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), bookResponseControllerKey{}, http.NewResponseController(w))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// WithBookWriteDeadlines is retained for existing CEX26.1 clients/tests.
// Both stream RPCs use the same HTTP/2 transport deadline middleware.
func WithBookWriteDeadlines(next http.Handler) http.Handler {
	return WithStreamWriteDeadlines(next)
}

// BookRPC is deliberately a small Connect adapter over the existing durable
// per-market hub. There is no independent Kafka consumer, journal cursor, or
// Redis-based replay path here.
type BookRPC struct {
	Registry *marketregistry.Registry
	Source   Source
	Hub      *Hub
	capacity chan struct{}
	// Set only by tests; production uses bookHeartbeatInterval.
	heartbeatInterval time.Duration
	mu                sync.Mutex
	closed            bool
	active            int
	stopped           chan struct{}
	stop              context.CancelFunc
	life              context.Context
}

func NewBookRPC(registry *marketregistry.Registry, source Source, hub *Hub, maxConcurrent int) (*BookRPC, error) {
	if registry == nil || source == nil || hub == nil || maxConcurrent < 1 || maxConcurrent > 10000 {
		return nil, errors.New("stream: invalid Connect book stream configuration")
	}
	life, stop := context.WithCancel(hub.parent)
	return &BookRPC{Registry: registry, Source: source, Hub: hub,
		capacity: make(chan struct{}, maxConcurrent), heartbeatInterval: bookHeartbeatInterval,
		life: life, stop: stop, stopped: make(chan struct{})}, nil
}

// ShutdownAndWait closes admission and waits for every accepted Connect stream
// to release its hub subscription and capacity slot. A deadline lets the
// caller force-close the HTTP server if a transport refuses to unblock.
func (b *BookRPC) ShutdownAndWait(ctx context.Context) error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.stop()
		if b.active == 0 {
			close(b.stopped)
		}
	}
	done := b.stopped
	b.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *BookRPC) enter() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.life.Err() != nil {
		return ErrStreamShutdown
	}
	select {
	case b.capacity <- struct{}{}:
		b.active++
		return nil
	default:
		return ErrSlowConsumer
	}
}

func (b *BookRPC) leave() {
	b.mu.Lock()
	<-b.capacity
	b.active--
	if b.closed && b.active == 0 {
		close(b.stopped)
	}
	b.mu.Unlock()
}

func (b *BookRPC) StreamOrderBook(ctx context.Context, request *connect.Request[api.StreamOrderBookRequest], output *connect.ServerStream[api.StreamOrderBookResponse]) error {
	if request == nil || request.Msg == nil || request.Msg.GetMarketId() == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("market_id is required"))
	}
	market, ok := b.Registry.BySymbol(request.Msg.GetMarketId())
	if !ok {
		return connect.NewError(connect.CodeNotFound, errors.New("unknown market"))
	}
	if request.Msg.GetDepth() != market.Depth {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("stream depth must equal configured materialized depth %d", market.Depth))
	}
	if ctx.Err() != nil {
		return bookRPCError(ctx.Err())
	}
	if err := b.enter(); err != nil {
		if errors.Is(err, ErrSlowConsumer) {
			return connect.NewError(connect.CodeResourceExhausted, errors.New("Connect book stream capacity exceeded"))
		}
		return bookRPCError(err)
	}
	defer b.leave()
	// Cancel in-flight PostgreSQL reads and writers when shutdown starts,
	// even if the client request remains open. HTTP send has its own deadline.
	streamCtx, cancelStream := context.WithCancel(ctx)
	stopOnShutdown := context.AfterFunc(b.life, cancelStream)
	defer func() { stopOnShutdown(); cancelStream() }()
	ctx = streamCtx

	// Subscribe before acquiring the snapshot; the bounded subscriber queue
	// captures events committed while the snapshot query is in flight.
	initialCtx, done := context.WithTimeout(ctx, bookInitialReadTimeout)
	sub, err := b.Hub.SubscribeBook(initialCtx, market)
	done()
	if err != nil {
		return bookRPCError(err)
	}
	defer sub.Close()

	initialCtx, done = context.WithTimeout(ctx, bookInitialReadTimeout)
	snap, err := b.Source.Snapshot(initialCtx, market)
	done()
	if err != nil {
		return bookRPCError(err)
	}
	if err := validateBookSnapshot(snap, market); err != nil {
		return bookRPCError(err)
	}
	if proto.Size(snap) > MaxSnapshotBytes {
		return bookRPCError(ErrPayloadTooLarge)
	}
	cursor := snap.GetMarketSequence()
	if err := sendBook(ctx, output, &api.StreamOrderBookResponse{
		Event: &api.StreamOrderBookResponse_Snapshot{Snapshot: snap},
	}, b.writeDeadline()); err != nil {
		return bookRPCError(err)
	}

	// Transport heartbeats do not advance the book cursor. They exercise the
	// write deadline on idle books and make broken/half-open peers observable.
	heartbeat := time.NewTicker(b.heartbeatInterval)
	defer heartbeat.Stop()
	lagTick := time.NewTicker(b.Hub.pollEvery)
	defer lagTick.Stop()
	var behindSince time.Time
	checkClientLag := func() error {
		head := sub.ObservedHead()
		if head < cursor {
			head = cursor
		} // snapshot can precede hub's next poll
		return checkLag(cursor, head, time.Now(), &behindSince, b.Hub.policy)
	}
	for {
		// Prefer the terminal queue error over any already-buffered deltas.
		// A lagging subscriber is never allowed to silently skip a sequence.
		select {
		case err := <-sub.Errors:
			return bookRPCError(err)
		default:
		}
		select {
		case <-ctx.Done():
			return bookRPCError(ctx.Err())
		case <-b.Hub.parent.Done():
			return connect.NewError(connect.CodeUnavailable, errors.New("market data shutting down"))
		case err := <-sub.Errors:
			return bookRPCError(err)
		case <-lagTick.C:
			if err := checkClientLag(); err != nil {
				return bookRPCError(err)
			}
		case feed := <-sub.Messages:
			feed.Release() // free queued-byte reservation before network I/O
			if feed.Result == nil || feed.Result.Delta == nil {
				return bookRPCError(ErrSequenceGap)
			}
			delta := feed.Result.Delta
			seq := delta.GetMarketSequence()
			if seq <= cursor { // buffered update already covered by snapshot
				continue
			}
			if seq-cursor != 1 || delta.GetPreviousMarketSequence() != cursor ||
				delta.GetMarketId() != market.Symbol || delta.GetSourceMarketId() != market.SourceID ||
				delta.GetMarketDefinitionVersion() != market.DefinitionVersion || delta.GetDepth() != market.Depth {
				return bookRPCError(ErrSequenceGap)
			}
			if proto.Size(delta) > MaxFeedBytes {
				return bookRPCError(ErrPayloadTooLarge)
			}
			if err := sendBook(ctx, output, &api.StreamOrderBookResponse{
				Event: &api.StreamOrderBookResponse_Delta{Delta: delta},
			}, b.writeDeadline()); err != nil {
				return bookRPCError(err)
			}
			cursor = seq // advance only after successful send
			if err := checkClientLag(); err != nil {
				return bookRPCError(err)
			}
		case <-heartbeat.C:
			if err := sendBook(ctx, output, &api.StreamOrderBookResponse{
				Event: &api.StreamOrderBookResponse_Heartbeat{Heartbeat: &api.StreamOrderBookHeartbeat{
					LastDeliveredMarketSequence: cursor,
				}},
			}, b.writeDeadline()); err != nil {
				return bookRPCError(err)
			}
		}
	}
}

func validateBookSnapshot(s *md.BookSnapshot, m marketregistry.Market) error {
	if s == nil || s.GetMarketSequence() == 0 ||
		s.GetMarketId() != m.Symbol || s.GetSourceMarketId() != m.SourceID ||
		s.GetMarketDefinitionVersion() != m.DefinitionVersion || s.GetDepth() != m.Depth {
		return errors.New("stream: invalid durable book snapshot")
	}
	return nil
}

// sendBook enforces a per-write deadline without limiting the overall stream
// lifetime. HTTP ResponseController is attached outside Connect's wrapper.
func (b *BookRPC) writeDeadline() time.Duration {
	if b.Hub.policy.MaxBehind < bookSendTimeout {
		return b.Hub.policy.MaxBehind
	}
	return bookSendTimeout
}

func sendBook(ctx context.Context, output *connect.ServerStream[api.StreamOrderBookResponse], response *api.StreamOrderBookResponse, timeout time.Duration) error {
	rc, ok := ctx.Value(bookResponseControllerKey{}).(*http.ResponseController)
	if !ok || rc == nil {
		return errors.New("stream: write deadline middleware missing")
	}
	if proto.Size(response) > MaxSnapshotBytes {
		return ErrPayloadTooLarge
	}
	if err := rc.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("stream: unsupported write deadline transport: %w", err)
	}
	err := output.Send(response)
	// Clear the absolute deadline between messages: idle markets must not
	// expire merely because no new matching result was committed.
	clearErr := rc.SetWriteDeadline(time.Time{})
	if err != nil {
		return err
	}
	return clearErr
}

func bookRPCError(err error) error {
	if err == nil {
		return nil
	}
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	case errors.Is(err, ErrSlowConsumer):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("slow book subscriber; reconnect for fresh snapshot"))
	case errors.Is(err, ErrPayloadTooLarge):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("book payload exceeds stream byte limit"))
	case errors.Is(err, ErrStreamShutdown):
		return connect.NewError(connect.CodeUnavailable, errors.New("market data shutting down"))
	case errors.Is(err, ErrStreamStale):
		return connect.NewError(connect.CodeUnavailable, errors.New("book stream stale; reconnect for fresh snapshot"))
	case errors.Is(err, ErrSequenceGap):
		return connect.NewError(connect.CodeUnavailable, errors.New("book sequence continuity lost; reconnect for fresh snapshot"))
	default:
		// Do not leak SQL queries, credentials or internal infrastructure.
		return connect.NewError(connect.CodeUnavailable, errors.New("book stream temporarily unavailable"))
	}
}
