package publicapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	"github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1/apiv1alpha1connect"
	marketv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeQueries struct{ start time.Time }

func (f fakeQueries) Book(_ context.Context, m marketregistry.Market, depth uint32) (*md.BookSnapshot, error) {
	return &md.BookSnapshot{MarketId: m.Symbol, SourceMarketId: m.SourceID, MarketSequence: 9, Depth: depth,
		MarketDefinitionVersion: m.DefinitionVersion, Bids: []*md.BookLevel{{PriceUnits: 123, BaseQuantityAtoms: "2000"}}}, nil
}
func (f fakeQueries) Trades(_ context.Context, m marketregistry.Market, limit uint32) ([]*md.TradeTick, error) {
	return []*md.TradeTick{{TradeId: "7:9:0", MarketId: m.Symbol, MatchSequence: 9, SourceMarketId: m.SourceID}}, nil
}
func (f fakeQueries) Ticker(_ context.Context, m marketregistry.Market, _ time.Time) (*md.Ticker, error) {
	return &md.Ticker{MarketId: m.Symbol, MarketSequence: 9}, nil
}
func (f fakeQueries) Stats(_ context.Context, m marketregistry.Market, _ time.Time) (*md.Stats24H, error) {
	return &md.Stats24H{MarketId: m.Symbol, BaseVolumeAtoms: "0"}, nil
}
func (f fakeQueries) Candles(_ context.Context, m marketregistry.Market, i md.CandleInterval, _, _ time.Time, after string, limit uint32) ([]*md.Candle, bool, error) {
	makeCandle := func(n int) *md.Candle {
		return &md.Candle{MarketId: m.Symbol, Interval: i, StartTime: timestamppb.New(f.start.Add(time.Duration(n) * time.Minute)), Revision: uint64(n)}
	}
	if after == "" {
		return []*md.Candle{makeCandle(0), makeCandle(1)}, true, nil
	}
	return []*md.Candle{makeCandle(2)}, false, nil
}
func newTestService(t *testing.T) *Service {
	t.Helper()
	r, err := marketregistry.New([]recovery.MarketConfig{{PartitionID: 0, Depth: 20, Definition: &md.MarketProjectionDefinition{
		SourceMarketId: 7, DefinitionVersion: 3, Market: &marketv1.Market{MarketId: "BTC-USDT"}}}})
	if err != nil {
		t.Fatal(err)
	}
	q := fakeQueries{start: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)}
	svc, err := NewService(r, q, []byte(strings.Repeat("a", 32)))
	if err != nil {
		t.Fatal(err)
	}
	svc.Clock = func() time.Time { return q.start }
	return svc
}
func TestConnectQueryHandlersAndSignedCandlePagination(t *testing.T) {
	svc := newTestService(t)
	prefix, handler := apiv1alpha1connect.NewMarketDataServiceHandler(svc)
	// The standard Connect handler must be mounted at its generated prefix.
	mux := http.NewServeMux()
	mux.Handle(prefix, handler)
	server := httptest.NewServer(mux)
	defer server.Close()
	client := apiv1alpha1connect.NewMarketDataServiceClient(server.Client(), server.URL)
	ctx := context.Background()
	book, err := client.GetOrderBook(ctx, connect.NewRequest(&api.GetOrderBookRequest{MarketId: "BTC-USDT", Depth: 5}))
	if err != nil || book.Msg.GetBook().GetDepth() != 5 {
		t.Fatalf("book query: %v %+v", err, book)
	}
	if _, err = client.GetOrderBook(ctx, connect.NewRequest(&api.GetOrderBookRequest{MarketId: "BTC-USDT", Depth: 21})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected invalid depth, got %v", err)
	}
	if _, err = client.GetTicker(ctx, connect.NewRequest(&api.GetTickerRequest{MarketId: "UNKNOWN"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("unknown market: %v", err)
	}
	if _, err = client.ListRecentTrades(ctx, connect.NewRequest(&api.ListRecentTradesRequest{MarketId: "BTC-USDT", Limit: 1001})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("trade bound: %v", err)
	}
	if stats, err := client.GetStats24H(ctx, connect.NewRequest(&api.GetStats24HRequest{MarketId: "BTC-USDT"})); err != nil || stats.Msg.GetStats() == nil {
		t.Fatalf("stats: %v", err)
	}
	start := timestamppb.New(svc.Clock())
	end := timestamppb.New(svc.Clock().Add(24 * time.Hour))
	req := &api.ListCandlesRequest{MarketId: "BTC-USDT", Interval: md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE,
		StartTimeInclusive: start, EndTimeExclusive: end, Limit: 2}
	first, err := client.ListCandles(ctx, connect.NewRequest(req))
	if err != nil || len(first.Msg.GetCandles()) != 2 || first.Msg.GetNextPageToken() == "" {
		t.Fatalf("first page: %v %+v", err, first)
	}
	req.PageToken = first.Msg.GetNextPageToken()
	second, err := client.ListCandles(ctx, connect.NewRequest(req))
	if err != nil || len(second.Msg.GetCandles()) != 1 || second.Msg.GetNextPageToken() != "" {
		t.Fatalf("second page: %v %+v", err, second)
	}
	req.Interval = md.CandleInterval_CANDLE_INTERVAL_ONE_DAY
	if _, err = client.ListCandles(ctx, connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("cursor must bind interval: %v", err)
	}
	req.Interval = md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE
	req.PageToken = req.PageToken + "A"
	if _, err = client.ListCandles(ctx, connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("tampered cursor: %v", err)
	}
}
func TestCursorSignatureAndBounds(t *testing.T) {
	key := []byte(strings.Repeat("s", 32))
	token, err := encodeCursor(key, candleCursor{V: 1, Market: "BTC-USDT", Interval: 1, Start: "0", End: "100", Last: "10", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeCursor(key, token); err != nil {
		t.Fatal(err)
	}
	if _, err = decodeCursor([]byte(strings.Repeat("x", 32)), token); err == nil {
		t.Fatal("signature accepted under wrong key")
	}
	if _, err = decodeCursor(key, strings.Repeat("A", 1100)); err == nil {
		t.Fatal("oversized cursor accepted")
	}
}
