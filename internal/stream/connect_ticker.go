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

const tickerHeartbeatInterval = 20 * time.Second

// TickerRPC serves a replaceable, latest-state observation. No replay or
// contiguous-delta guarantee exists for ticker: market_sequence MUST increase
// strictly, but can jump over any number of matching results. Reconnection
// always restarts with a fresh durable snapshot.
type TickerRPC struct {
	Registry          *marketregistry.Registry
	Source            TickerSource
	Hub               *Hub
	capacity          chan struct{}
	heartbeatInterval time.Duration
	mu                sync.Mutex
	closed            bool
	active            int
	stopped           chan struct{}
	life              context.Context
	stop              context.CancelFunc
}

func NewTickerRPC(registry *marketregistry.Registry, hub *Hub, maxConcurrent int) (*TickerRPC, error) {
	if registry == nil || hub == nil || maxConcurrent < 1 || maxConcurrent > 10000 {
		return nil, errors.New("stream: invalid Connect ticker stream configuration")
	}
	// Initial observations and subsequent hub publications MUST use the
	// same authoritative ticker source. Do not accept a second reader that
	// could point at a different topic/database or projection epoch.
	reader, ok := hub.source.(TickerSource)
	if !ok {
		return nil, errors.New("stream: hub source lacks authoritative ticker")
	}
	life, stop := context.WithCancel(hub.parent)
	return &TickerRPC{Registry: registry, Source: reader, Hub: hub,
		capacity: make(chan struct{}, maxConcurrent), heartbeatInterval: tickerHeartbeatInterval,
		life: life, stop: stop, stopped: make(chan struct{})}, nil
}

func (t *TickerRPC) ShutdownAndWait(ctx context.Context) error {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		t.stop()
		if t.active == 0 {
			close(t.stopped)
		}
	}
	done := t.stopped
	t.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *TickerRPC) enter() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.life.Err() != nil {
		return ErrStreamShutdown
	}
	select {
	case t.capacity <- struct{}{}:
		t.active++
		return nil
	default:
		return ErrSlowConsumer
	}
}

func (t *TickerRPC) leave() {
	t.mu.Lock()
	<-t.capacity
	t.active--
	if t.closed && t.active == 0 {
		close(t.stopped)
	}
	t.mu.Unlock()
}

func validateStreamTicker(ticker *md.Ticker, market marketregistry.Market) error {
	if ticker == nil || ticker.GetMarketId() != market.Symbol || ticker.GetMarketSequence() == 0 ||
		ticker.GetUpdatedAt() == nil || ticker.GetUpdatedAt().CheckValid() != nil ||
		(ticker.LastPriceUnits != nil && ticker.GetLastPriceUnits() == 0) ||
		(ticker.BestBidPriceUnits != nil && ticker.GetBestBidPriceUnits() == 0) ||
		(ticker.BestAskPriceUnits != nil && ticker.GetBestAskPriceUnits() == 0) ||
		(ticker.BestBidPriceUnits != nil && ticker.BestAskPriceUnits != nil && ticker.GetBestBidPriceUnits() >= ticker.GetBestAskPriceUnits()) {
		return ErrSequenceGap
	}
	return nil
}

func (t *TickerRPC) StreamTicker(ctx context.Context, req *connect.Request[api.StreamTickerRequest], out *connect.ServerStream[api.StreamTickerResponse]) error {
	if req == nil || req.Msg == nil || req.Msg.GetMarketId() == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("market_id is required"))
	}
	market, ok := t.Registry.BySymbol(req.Msg.GetMarketId())
	if !ok {
		return connect.NewError(connect.CodeNotFound, errors.New("unknown market"))
	}
	if err := ctx.Err(); err != nil {
		return tickerRPCError(err)
	}
	if err := t.enter(); err != nil {
		if errors.Is(err, ErrSlowConsumer) {
			return connect.NewError(connect.CodeResourceExhausted, errors.New("Connect ticker stream capacity exceeded"))
		}
		return tickerRPCError(err)
	}
	defer t.leave()
	streamCtx, cancel := context.WithCancel(ctx)
	stopOnShutdown := context.AfterFunc(t.life, cancel)
	defer func() { stopOnShutdown(); cancel() }()
	ctx = streamCtx

	// Register FIRST, then read the durable snapshot. A concurrent commit
	// is either in the pending latest-state slot or observed on a later poll.
	initialCtx, done := context.WithTimeout(ctx, bookInitialReadTimeout)
	sub, err := t.Hub.SubscribeTicker(initialCtx, market)
	done()
	if err != nil {
		return tickerRPCError(err)
	}
	defer sub.Close()
	initialCtx, done = context.WithTimeout(ctx, bookInitialReadTimeout)
	snapshot, err := t.Source.LatestTicker(initialCtx, market)
	done()
	if err != nil {
		return tickerRPCError(err)
	}
	if err := validateStreamTicker(snapshot, market); err != nil {
		return tickerRPCError(err)
	}
	cursor := snapshot.GetMarketSequence()
	if err := sendTicker(ctx, out, &api.StreamTickerResponse{
		MarketDefinitionVersion: market.DefinitionVersion,
		Event:                   &api.StreamTickerResponse_Snapshot{Snapshot: snapshot},
	}, t.writeDeadline()); err != nil {
		return tickerRPCError(err)
	}

	heartbeat := time.NewTicker(t.heartbeatInterval)
	defer heartbeat.Stop()
	for {
		// Terminal queue failures take precedence over pending coalesced data.
		select {
		case err := <-sub.Errors:
			return tickerRPCError(err)
		default:
		}
		select {
		case <-ctx.Done():
			return tickerRPCError(ctx.Err())
		case <-t.Hub.parent.Done():
			return tickerRPCError(ErrStreamShutdown)
		case err := <-sub.Errors:
			return tickerRPCError(err)
		case feed := <-sub.Messages:
			feed.Release()
			if err := validateStreamTicker(feed.Ticker, market); err != nil {
				return tickerRPCError(err)
			}
			seq := feed.Ticker.GetMarketSequence()
			if seq <= cursor {
				continue // old buffered state; never regress or re-emit
			}
			if err := sendTicker(ctx, out, &api.StreamTickerResponse{
				MarketDefinitionVersion: market.DefinitionVersion,
				Event:                   &api.StreamTickerResponse_Update{Update: feed.Ticker},
			}, t.writeDeadline()); err != nil {
				return tickerRPCError(err)
			}
			cursor = seq // only advance after a successful network write
		case <-heartbeat.C:
			if err := sendTicker(ctx, out, &api.StreamTickerResponse{
				MarketDefinitionVersion: market.DefinitionVersion,
				Event: &api.StreamTickerResponse_Heartbeat{Heartbeat: &api.StreamTickerHeartbeat{
					LastDeliveredMarketSequence: cursor,
				}},
			}, t.writeDeadline()); err != nil {
				return tickerRPCError(err)
			}
		}
	}
}

func (t *TickerRPC) writeDeadline() time.Duration {
	if t.Hub.policy.MaxBehind < bookSendTimeout {
		return t.Hub.policy.MaxBehind
	}
	return bookSendTimeout
}

func sendTicker(ctx context.Context, out *connect.ServerStream[api.StreamTickerResponse], msg *api.StreamTickerResponse, timeout time.Duration) error {
	rc, ok := ctx.Value(bookResponseControllerKey{}).(*http.ResponseController)
	if !ok || rc == nil {
		return errors.New("stream: write deadline middleware missing")
	}
	if proto.Size(msg) > MaxFeedBytes {
		return ErrPayloadTooLarge
	}
	if err := rc.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("stream: ticker write deadline unavailable: %w", err)
	}
	err := out.Send(msg)
	clearErr := rc.SetWriteDeadline(time.Time{})
	if err != nil {
		return err
	}
	return clearErr
}

func tickerRPCError(err error) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	case errors.Is(err, ErrSlowConsumer), errors.Is(err, ErrPayloadTooLarge):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("ticker stream capacity exceeded; reconnect for a fresh snapshot"))
	case errors.Is(err, ErrStreamShutdown):
		return connect.NewError(connect.CodeUnavailable, errors.New("market data shutting down"))
	default:
		return connect.NewError(connect.CodeUnavailable, errors.New("ticker projection unavailable; reconnect for a fresh snapshot"))
	}
}
