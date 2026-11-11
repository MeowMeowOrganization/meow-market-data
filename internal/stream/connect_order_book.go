package stream

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
)

const (
	bookInitialReadTimeout = 5 * time.Second
	bookSendTimeout        = 5 * time.Second
	bookHeartbeatInterval  = 20 * time.Second
)

// bookResponseControllerKey is set only by WithBookWriteDeadlines, which must
// wrap the generated Connect handler. Deadline enforcement is not optional:
// otherwise a peer that stops reading can block a stream.Send indefinitely.
type bookResponseControllerKey struct{}

// WithBookWriteDeadlines exposes the HTTP response controller to the RPC
// method without changing Connect's generated interfaces. Do not wrap the
// ResponseWriter with an unwrappable custom writer: ResponseController needs
// the underlying HTTP/1 or HTTP/2 transport's SetWriteDeadline support.
func WithBookWriteDeadlines(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), bookResponseControllerKey{}, http.NewResponseController(w))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
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
}

func NewBookRPC(registry *marketregistry.Registry, source Source, hub *Hub, maxConcurrent int) (*BookRPC, error) {
	if registry == nil || source == nil || hub == nil || maxConcurrent < 1 || maxConcurrent > 10000 {
		return nil, errors.New("stream: invalid Connect book stream configuration")
	}
	return &BookRPC{Registry: registry, Source: source, Hub: hub,
		capacity: make(chan struct{}, maxConcurrent), heartbeatInterval: bookHeartbeatInterval}, nil
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
	select {
	case <-b.Hub.parent.Done():
		return connect.NewError(connect.CodeUnavailable, errors.New("market data shutting down"))
	case b.capacity <- struct{}{}:
		defer func() { <-b.capacity }()
	default:
		return connect.NewError(connect.CodeResourceExhausted, errors.New("Connect book stream capacity exceeded"))
	}

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
	cursor := snap.GetMarketSequence()
	if err := sendBook(ctx, output, &api.StreamOrderBookResponse{
		Event: &api.StreamOrderBookResponse_Snapshot{Snapshot: snap},
	}); err != nil {
		return bookRPCError(err)
	}

	// Transport heartbeats do not advance the book cursor. They exercise the
	// write deadline on idle books and make broken/half-open peers observable.
	heartbeat := time.NewTicker(b.heartbeatInterval)
	defer heartbeat.Stop()
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
		case feed := <-sub.Messages:
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
			if err := sendBook(ctx, output, &api.StreamOrderBookResponse{
				Event: &api.StreamOrderBookResponse_Delta{Delta: delta},
			}); err != nil {
				return bookRPCError(err)
			}
			cursor = seq // advance only after successful send
		case <-heartbeat.C:
			if err := sendBook(ctx, output, &api.StreamOrderBookResponse{
				Event: &api.StreamOrderBookResponse_Heartbeat{Heartbeat: &api.StreamOrderBookHeartbeat{
					LastDeliveredMarketSequence: cursor,
				}},
			}); err != nil {
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
func sendBook(ctx context.Context, output *connect.ServerStream[api.StreamOrderBookResponse], response *api.StreamOrderBookResponse) error {
	rc, ok := ctx.Value(bookResponseControllerKey{}).(*http.ResponseController)
	if !ok || rc == nil {
		return errors.New("stream: write deadline middleware missing")
	}
	if err := rc.SetWriteDeadline(time.Now().Add(bookSendTimeout)); err != nil {
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
	case errors.Is(err, ErrSequenceGap):
		return connect.NewError(connect.CodeUnavailable, errors.New("book sequence continuity lost; reconnect for fresh snapshot"))
	default:
		// Do not leak SQL queries, credentials or internal infrastructure.
		return connect.NewError(connect.CodeUnavailable, errors.New("book stream temporarily unavailable"))
	}
}
