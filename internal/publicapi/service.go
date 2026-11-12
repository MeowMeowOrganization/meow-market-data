package publicapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	"github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1/apiv1alpha1connect"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	"github.com/MeowMeowOrganization/meow-market-data/internal/stream"
)

type Queries interface {
	Book(context.Context, marketregistry.Market, uint32) (*md.BookSnapshot, error)
	Trades(context.Context, marketregistry.Market, uint32) ([]*md.TradeTick, error)
	Ticker(context.Context, marketregistry.Market, time.Time) (*md.Ticker, error)
	Stats(context.Context, marketregistry.Market, time.Time) (*md.Stats24H, error)
	Candles(context.Context, marketregistry.Market, md.CandleInterval, time.Time, time.Time, string, uint32) ([]*md.Candle, bool, error)
}

type Service struct {
	apiv1alpha1connect.UnimplementedMarketDataServiceHandler
	Registry     *marketregistry.Registry
	Queries      Queries
	BookStream   *stream.BookRPC
	TradeStream  *stream.TradeRPC
	CursorSecret []byte
	Clock        func() time.Time
}

func NewService(registry *marketregistry.Registry, q Queries, secret []byte) (*Service, error) {
	if registry == nil || q == nil || len(secret) < 32 {
		return nil, errors.New("publicapi: registry, queries and stable >=32-byte cursor key required")
	}
	return &Service{Registry: registry, Queries: q, CursorSecret: append([]byte(nil), secret...), Clock: time.Now}, nil
}
func (s *Service) market(symbol string) (marketregistry.Market, error) {
	if symbol == "" {
		return marketregistry.Market{}, connect.NewError(connect.CodeInvalidArgument, errors.New("market_id is required"))
	}
	m, ok := s.Registry.BySymbol(symbol)
	if !ok {
		return m, connect.NewError(connect.CodeNotFound, errors.New("unknown market"))
	}
	return m, nil
}
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotReady) {
		return connect.NewError(connect.CodeUnavailable, errors.New("market projection is not ready"))
	}
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	// Do not leak SQL, Redis or internal configuration details to public RPC clients.
	return connect.NewError(connect.CodeUnavailable, errors.New("market-data temporarily unavailable"))
}
func (s *Service) GetOrderBook(ctx context.Context, req *connect.Request[api.GetOrderBookRequest]) (*connect.Response[api.GetOrderBookResponse], error) {
	m, err := s.market(req.Msg.GetMarketId())
	if err != nil {
		return nil, err
	}
	depth := req.Msg.GetDepth()
	if depth == 0 || depth > m.Depth {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("depth must be between 1 and %d", m.Depth))
	}
	b, err := s.Queries.Book(ctx, m, depth)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&api.GetOrderBookResponse{Book: b}), nil
}
func (s *Service) ListRecentTrades(ctx context.Context, req *connect.Request[api.ListRecentTradesRequest]) (*connect.Response[api.ListRecentTradesResponse], error) {
	m, err := s.market(req.Msg.GetMarketId())
	if err != nil {
		return nil, err
	}
	limit := req.Msg.GetLimit()
	if limit == 0 {
		limit = 100
	}
	if limit > 1000 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("limit must be <= 1000"))
	}
	t, err := s.Queries.Trades(ctx, m, limit)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&api.ListRecentTradesResponse{Trades: t}), nil
}
func (s *Service) GetTicker(ctx context.Context, req *connect.Request[api.GetTickerRequest]) (*connect.Response[api.GetTickerResponse], error) {
	m, err := s.market(req.Msg.GetMarketId())
	if err != nil {
		return nil, err
	}
	t, err := s.Queries.Ticker(ctx, m, s.Clock().UTC())
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&api.GetTickerResponse{Ticker: t}), nil
}
func (s *Service) GetStats24H(ctx context.Context, req *connect.Request[api.GetStats24HRequest]) (*connect.Response[api.GetStats24HResponse], error) {
	m, err := s.market(req.Msg.GetMarketId())
	if err != nil {
		return nil, err
	}
	stats, err := s.Queries.Stats(ctx, m, s.Clock().UTC())
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&api.GetStats24HResponse{Stats: stats}), nil
}
func (s *Service) ListCandles(ctx context.Context, req *connect.Request[api.ListCandlesRequest]) (*connect.Response[api.ListCandlesResponse], error) {
	r := req.Msg
	m, err := s.market(r.GetMarketId())
	if err != nil {
		return nil, err
	}
	switch r.GetInterval() {
	case md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES,
		md.CandleInterval_CANDLE_INTERVAL_FIFTEEN_MINUTES, md.CandleInterval_CANDLE_INTERVAL_ONE_HOUR,
		md.CandleInterval_CANDLE_INTERVAL_ONE_DAY:
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unsupported interval"))
	}
	if r.GetStartTimeInclusive() == nil || r.GetEndTimeExclusive() == nil ||
		r.GetStartTimeInclusive().CheckValid() != nil || r.GetEndTimeExclusive().CheckValid() != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("valid start/end timestamps required"))
	}
	start, end := r.GetStartTimeInclusive().AsTime().UTC(), r.GetEndTimeExclusive().AsTime().UTC()
	if !start.Before(end) || end.Sub(start) > 366*24*time.Hour {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("range must be positive and <= 366 days"))
	}
	limit := r.GetLimit()
	if limit == 0 {
		limit = 200
	}
	if limit > 500 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("limit must be <= 500"))
	}
	startNS, endNS := postgres.NanosDecimal(start), postgres.NanosDecimal(end)
	after := ""
	if r.GetPageToken() != "" {
		cursor, e := decodeCursor(s.CursorSecret, r.GetPageToken())
		if e != nil || cursor.Market != m.Symbol || cursor.Interval != int32(r.GetInterval()) || cursor.Start != startNS || cursor.End != endNS || cursor.Limit != limit {
			return nil, connect.NewError(connect.CodeInvalidArgument, errCursor)
		}
		after = cursor.Last
	}
	candles, more, err := s.Queries.Candles(ctx, m, r.GetInterval(), start, end, after, limit)
	if err != nil {
		return nil, rpcError(err)
	}
	result := &api.ListCandlesResponse{Candles: candles}
	if more && len(candles) > 0 {
		last := candles[len(candles)-1]
		if last.GetStartTime() == nil {
			return nil, rpcError(ErrCorrupt)
		}
		token, e := encodeCursor(s.CursorSecret, candleCursor{V: 1, Market: m.Symbol, Interval: int32(r.GetInterval()), Start: startNS, End: endNS, Last: postgres.NanosDecimal(last.GetStartTime().AsTime()), Limit: limit})
		if e != nil {
			return nil, rpcError(e)
		}
		result.NextPageToken = token
	}
	return connect.NewResponse(result), nil
}
