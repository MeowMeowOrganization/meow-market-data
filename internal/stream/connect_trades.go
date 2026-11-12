package stream

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-contracts/tradeid"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"google.golang.org/protobuf/proto"
)

const (
	MaxTradeReplaySequences   uint64 = 20000
	tradeHeartbeatInterval           = 20 * time.Second
	maxTradeConcurrentReplays        = 32
	maxTradeReplayDuration           = 30 * time.Second
)

// TradeRPC shares the book hub's transactionally consistent PostgreSQL journal.
// It owns only admission and HTTP request lifecycle; there is no second poller.
type TradeRPC struct {
	Registry          *marketregistry.Registry
	Source            Source
	Hub               *Hub
	capacity          chan struct{}
	replayCapacity    chan struct{}
	heartbeatInterval time.Duration
	mu                sync.Mutex
	closed            bool
	active            int
	stopped           chan struct{}
	life              context.Context
	stop              context.CancelFunc
}

func NewTradeRPC(registry *marketregistry.Registry, source Source, hub *Hub, maxConcurrent int) (*TradeRPC, error) {
	if registry == nil || source == nil || hub == nil || maxConcurrent < 1 || maxConcurrent > 10000 {
		return nil, errors.New("stream: invalid Connect trade stream configuration")
	}
	life, stop := context.WithCancel(hub.parent)
	return &TradeRPC{Registry: registry, Source: source, Hub: hub, capacity: make(chan struct{}, maxConcurrent),
		replayCapacity:    make(chan struct{}, min(maxConcurrent, maxTradeConcurrentReplays)),
		heartbeatInterval: tradeHeartbeatInterval, life: life, stop: stop, stopped: make(chan struct{})}, nil
}

// ShutdownAndWait atomically refuses admission and joins all accepted RPCs.
func (t *TradeRPC) ShutdownAndWait(ctx context.Context) error {
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
func (t *TradeRPC) enter() error {
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
func (t *TradeRPC) leave() {
	t.mu.Lock()
	<-t.capacity
	t.active--
	if t.closed && t.active == 0 {
		close(t.stopped)
	}
	t.mu.Unlock()
}

// validateTradeBatch validates source provenance and total ordinal order; the
// immutable trade_id is not an invented transport cursor.
func validateTradeBatch(trades []*md.TradeTick, m marketregistry.Market, sequence uint64) error {
	var last uint64
	for i, tr := range trades {
		if tr == nil || tr.GetMarketId() != m.Symbol || tr.GetSourceMarketId() != m.SourceID ||
			tr.GetMarketDefinitionVersion() != m.DefinitionVersion || tr.GetMatchSequence() != sequence {
			return ErrSequenceGap
		}
		idMarket, idSeq, idOrd, err := tradeid.Parse(tr.GetTradeId())
		if err != nil || idMarket != m.SourceID || idSeq != sequence || idOrd != tr.GetEventOrdinal() ||
			(i > 0 && tr.GetEventOrdinal() <= last) {
			return ErrSequenceGap
		}
		last = tr.GetEventOrdinal()
	}
	return nil
}

// loadTradeResume rejects untrusted/expired cursors before starting a stream.
// A partial cursor must name a REAL execution within the replayable journal,
// not merely a plausible ordinal that could skip legitimate executions.
func loadTradeResume(ctx context.Context, source Source, m marketregistry.Market, cursor *api.TradeStreamCursor, head, floor uint64) (uint64, error) {
	seq := cursor.GetMarketSequence()
	if seq == 0 || seq > head || cursor.GetMarketDefinitionVersion() != m.DefinitionVersion ||
		(cursor.GetSequenceComplete() && cursor.GetEventOrdinal() != 0) {
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid trade resume cursor or market definition version"))
	}
	if cursor.GetSequenceComplete() {
		// A completed cursor before the floor is valid only at floor-1.
		if seq < floor && (floor == 0 || seq != floor-1) {
			return 0, connect.NewError(connect.CodeFailedPrecondition, errors.New("trade cursor is outside durable replay retention; historical backfill required"))
		}
		return seq, nil
	}
	if seq < floor {
		return 0, connect.NewError(connect.CodeFailedPrecondition, errors.New("trade cursor is outside durable replay retention; historical backfill required"))
	}
	readCtx, cancel := context.WithTimeout(ctx, bookInitialReadTimeout)
	defer cancel()
	results, _, err := boundedResults(readCtx, source, m, seq-1, seq, 1)
	if err != nil {
		return 0, tradeRPCError(err)
	}
	if len(results) != 1 || results[0].Delta == nil || results[0].Delta.GetMarketSequence() != seq {
		return 0, tradeRPCError(ErrSequenceGap)
	}
	if err := validateTradeBatch(results[0].Trades, m, seq); err != nil {
		return 0, tradeRPCError(err)
	}
	for _, trade := range results[0].Trades {
		if trade.GetEventOrdinal() == cursor.GetEventOrdinal() {
			return seq - 1, nil
		}
	}
	return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("trade cursor does not identify a committed execution"))
}

func (t *TradeRPC) StreamTrades(ctx context.Context, request *connect.Request[api.StreamTradesRequest], output *connect.ServerStream[api.StreamTradesResponse]) error {
	if request == nil || request.Msg == nil || request.Msg.GetMarketId() == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("market_id is required"))
	}
	market, ok := t.Registry.BySymbol(request.Msg.GetMarketId())
	if !ok {
		return connect.NewError(connect.CodeNotFound, errors.New("unknown market"))
	}
	if ctx.Err() != nil {
		return tradeRPCError(ctx.Err())
	}
	if err := t.enter(); err != nil {
		if errors.Is(err, ErrSlowConsumer) {
			return connect.NewError(connect.CodeResourceExhausted, errors.New("Connect trade stream capacity exceeded"))
		}
		return tradeRPCError(err)
	}
	defer t.leave()
	streamCtx, cancel := context.WithCancel(ctx)
	stopOnShutdown := context.AfterFunc(t.life, cancel)
	defer func() { stopOnShutdown(); cancel() }()
	ctx = streamCtx
	// Independent admission budget keeps a reconnect storm from creating
	// hundreds of simultaneous multi-batch PostgreSQL replay scans.
	if request.Msg.GetResumeAfter() != nil {
		select {
		case t.replayCapacity <- struct{}{}:
			defer func() { <-t.replayCapacity }()
		default:
			return connect.NewError(connect.CodeResourceExhausted, errors.New("trade replay capacity exceeded; retry later"))
		}
	}

	// Subscribe before fixing the PostgreSQL head watermark: all later events
	// are either buffered or remain retrievable from the journal.
	initCtx, done := context.WithTimeout(ctx, bookInitialReadTimeout)
	sub, err := t.Hub.SubscribeTrades(initCtx, market)
	done()
	if err != nil {
		return tradeRPCError(err)
	}
	defer sub.Close()
	initCtx, done = context.WithTimeout(ctx, bookInitialReadTimeout)
	head, err := t.Source.Head(initCtx, market)
	done()
	if err != nil {
		return tradeRPCError(err)
	}
	initCtx, done = context.WithTimeout(ctx, bookInitialReadTimeout)
	floor, err := t.Source.ReplayFloor(initCtx, market)
	done()
	if err != nil {
		return tradeRPCError(err)
	}
	if head == 0 || floor == 0 || floor > head || head == math.MaxUint64 {
		return tradeRPCError(ErrSequenceGap)
	}

	mode := api.StreamTradesStarted_MODE_LIVE
	replayAfter := head
	resume := request.Msg.GetResumeAfter()
	if resume != nil {
		replayAfter, err = loadTradeResume(ctx, t.Source, market, resume, head, floor)
		if err != nil {
			return err
		}
		if head-replayAfter > MaxTradeReplaySequences {
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("trade replay window exceeded; historical backfill required"))
		}
		mode = api.StreamTradesStarted_MODE_REPLAY
	}
	cursor := replayAfter
	var replayDeadline time.Time
	if resume != nil {
		replayDeadline = time.Now().Add(maxTradeReplayDuration)
	}
	checkReplayDeadline := func() error {
		if !replayDeadline.IsZero() && time.Now().After(replayDeadline) {
			return context.DeadlineExceeded
		}
		return nil
	}
	if err := sendTrade(ctx, output, &api.StreamTradesResponse{Event: &api.StreamTradesResponse_Started{Started: &api.StreamTradesStarted{
		Mode: mode, HeadMarketSequence: head, ReplayFloorMarketSequence: floor, MarketDefinitionVersion: market.DefinitionVersion,
	}}}, t.writeDeadline()); err != nil {
		return tradeRPCError(err)
	}

	// sendBatch writes executions in ordinal order and only then completes the
	// sequence. A client may checkpoint either a specific trade or sequence_end.
	sendBatch := func(seq uint64, trades []*md.TradeTick, partial *api.TradeStreamCursor) error {
		if err := validateTradeBatch(trades, market, seq); err != nil {
			return err
		}
		for _, tick := range trades {
			if err := checkReplayDeadline(); err != nil {
				return err
			}
			if partial != nil && seq == partial.GetMarketSequence() && !partial.GetSequenceComplete() && tick.GetEventOrdinal() <= partial.GetEventOrdinal() {
				continue
			}
			select {
			case e := <-sub.Errors:
				return e
			default:
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := sendTrade(ctx, output, &api.StreamTradesResponse{Event: &api.StreamTradesResponse_Trade{Trade: tick}}, t.writeDeadline()); err != nil {
				return err
			}
		}
		if len(trades) > 0 {
			select {
			case e := <-sub.Errors:
				return e
			default:
			}
			if err := sendTrade(ctx, output, &api.StreamTradesResponse{Event: &api.StreamTradesResponse_SequenceEnd{SequenceEnd: &api.StreamTradesSequenceEnd{
				MarketSequence: seq, MarketDefinitionVersion: market.DefinitionVersion,
			}}}, t.writeDeadline()); err != nil {
				return err
			}
		}
		cursor = seq // only after writing all executions and the completion marker
		return nil
	}

	if resume != nil {
		// Bounded replay, with DB consistency per batch and strict sequence
		// validation across batches. A concurrent truncation fails closed.
		for cursor < head {
			if err := checkReplayDeadline(); err != nil {
				return tradeRPCError(err)
			}
			select {
			case e := <-sub.Errors:
				return tradeRPCError(e)
			default:
			}
			if ctx.Err() != nil {
				return tradeRPCError(ctx.Err())
			}
			readCtx, stop := context.WithTimeout(ctx, bookInitialReadTimeout)
			results, _, e := boundedResults(readCtx, t.Source, market, cursor, head, 256)
			stop()
			if e != nil {
				return tradeRPCError(e)
			}
			if len(results) == 0 {
				return tradeRPCError(ErrSequenceGap)
			}
			for _, result := range results {
				if err := checkReplayDeadline(); err != nil {
					return tradeRPCError(err)
				}
				if result.Delta == nil || result.Delta.GetMarketSequence() != cursor+1 ||
					result.Delta.GetPreviousMarketSequence() != cursor ||
					result.Delta.GetMarketId() != market.Symbol || result.Delta.GetSourceMarketId() != market.SourceID ||
					result.Delta.GetMarketDefinitionVersion() != market.DefinitionVersion {
					return tradeRPCError(ErrSequenceGap)
				}
				if err := sendBatch(cursor+1, result.Trades, resume); err != nil {
					return tradeRPCError(err)
				}
			}
		}
	}

	// Replay duration budget applies only to catch-up, not the subsequent
	// long-lived live stream.
	replayDeadline = time.Time{}
	heartbeat := time.NewTicker(t.heartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case e := <-sub.Errors:
			return tradeRPCError(e)
		default:
		}
		select {
		case <-ctx.Done():
			return tradeRPCError(ctx.Err())
		case <-t.Hub.parent.Done():
			return tradeRPCError(ErrStreamShutdown)
		case e := <-sub.Errors:
			return tradeRPCError(e)
		case f := <-sub.Messages:
			f.Release()
			if f.Trades == nil || f.Trades.MarketSequence == 0 || len(f.Trades.Trades) == 0 {
				return tradeRPCError(ErrSequenceGap)
			}
			if f.Trades.MarketSequence <= cursor {
				continue
			} // included in replay/initial head
			if err := sendBatch(f.Trades.MarketSequence, f.Trades.Trades, nil); err != nil {
				return tradeRPCError(err)
			}
		case <-heartbeat.C:
			if err := sendTrade(ctx, output, &api.StreamTradesResponse{Event: &api.StreamTradesResponse_Heartbeat{Heartbeat: &api.StreamTradesHeartbeat{
				LastCompletedMarketSequence: cursor,
			}}}, t.writeDeadline()); err != nil {
				return tradeRPCError(err)
			}
		}
	}
}

func (t *TradeRPC) writeDeadline() time.Duration {
	if t.Hub.policy.MaxBehind < bookSendTimeout {
		return t.Hub.policy.MaxBehind
	}
	return bookSendTimeout
}

func sendTrade(ctx context.Context, out *connect.ServerStream[api.StreamTradesResponse], response *api.StreamTradesResponse, timeout time.Duration) error {
	rc, ok := ctx.Value(bookResponseControllerKey{}).(*http.ResponseController)
	if !ok || rc == nil {
		return errors.New("stream: write deadline middleware missing")
	}
	if proto.Size(response) > MaxFeedBytes {
		return ErrPayloadTooLarge
	}
	if err := rc.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("stream: write deadline unavailable: %w", err)
	}
	err := out.Send(response)
	clearErr := rc.SetWriteDeadline(time.Time{})
	if err != nil {
		return err
	}
	return clearErr
}

func tradeRPCError(err error) error {
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
	case errors.Is(err, ErrSlowConsumer):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("slow trade subscriber; reconnect from last persisted trade cursor"))
	case errors.Is(err, ErrPayloadTooLarge):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("trade payload exceeds stream byte budget"))
	case errors.Is(err, ErrStreamShutdown):
		return connect.NewError(connect.CodeUnavailable, errors.New("market data shutting down"))
	case errors.Is(err, ErrStreamStale):
		return connect.NewError(connect.CodeUnavailable, errors.New("trade stream stale; resume from persisted cursor"))
	case errors.Is(err, ErrSequenceGap):
		return connect.NewError(connect.CodeUnavailable, errors.New("trade journal continuity invalid; resume from persisted cursor"))
	default:
		return connect.NewError(connect.CodeUnavailable, errors.New("trade stream temporarily unavailable"))
	}
}
