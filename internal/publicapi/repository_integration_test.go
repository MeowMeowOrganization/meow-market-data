//go:build integration

package publicapi

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPublicQueriesWithRealPostgres(t *testing.T) {
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires MARKET_DATA_TEST_DATABASE_URL, migrations 000001..000003")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	topic := fmt.Sprintf("query-integration-%d", time.Now().UnixNano())
	id := uint32(9099999)
	m := marketregistry.Market{Symbol: "CHECK-USD", SourceID: id, Depth: 20, DefinitionVersion: 3}
	start := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	snap := &md.BookSnapshot{MarketId: m.Symbol, SourceMarketId: id, MarketSequence: 5, MarketDefinitionVersion: 3, Depth: 20,
		Bids: []*md.BookLevel{{PriceUnits: 100, BaseQuantityAtoms: "18446744073709551616000"}}}
	data, err := proto.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO market_data.public_books
        (topic,source_market_id,market_id,market_sequence,market_definition_version,depth,recorded_at_ns,snapshot)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, topic, int64(id), m.Symbol, "5", "3", 20, postgres.NanosDecimal(start), data); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 3; n++ {
		when := start.Add(time.Duration(n) * time.Minute)
		tr := &md.TradeTick{TradeId: fmt.Sprintf("%d:%d:0", id, n+3), MarketId: m.Symbol,
			SourceMarketId: id, MatchSequence: uint64(n + 3), EventOrdinal: 0, PriceUnits: uint64(101 + n), BaseQuantityAtoms: 1000,
			QuoteQuantityAtoms: 1, AggressorSide: orderv1.OrderSide_ORDER_SIDE_BUY, RecordedAt: timestamppb.New(when)}
		raw, e := proto.Marshal(tr)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.ExecContext(ctx, `INSERT INTO market_data.public_trades
            (topic,source_market_id,market_id,market_sequence,event_ordinal,trade_id,recorded_at_ns,price_units,base_atoms,quote_atoms,payload)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, topic, int64(id), m.Symbol,
			fmt.Sprint(n+3), "0", tr.TradeId, postgres.NanosDecimal(when), fmt.Sprint(tr.PriceUnits), "1000", "1", raw); e != nil {
			t.Fatal(e)
		}
		end := when.Add(time.Minute)
		candle := &md.Candle{MarketId: m.Symbol, Interval: md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE,
			StartTime: timestamppb.New(when), EndTime: timestamppb.New(end), OpenPriceUnits: tr.PriceUnits,
			HighPriceUnits: tr.PriceUnits, LowPriceUnits: tr.PriceUnits, ClosePriceUnits: tr.PriceUnits,
			BaseVolumeAtoms: "1000", QuoteVolumeAtoms: "1", TradeCount: 1}
		encoded, e := proto.Marshal(candle)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.ExecContext(ctx, `INSERT INTO market_data.candles
            (topic,source_market_id,market_id,interval_id,start_ns,end_ns,revision,payload)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, topic, int64(id), m.Symbol, 1,
			postgres.NanosDecimal(when), postgres.NanosDecimal(end), 0, encoded); e != nil {
			t.Fatal(e)
		}
	}
	repo := &Repository{DB: db, Topic: topic}
	book, err := repo.Book(ctx, m, 2)
	if err != nil || book.GetDepth() != 2 || book.Bids[0].BaseQuantityAtoms != "18446744073709551616000" {
		t.Fatalf("book exact integer: %+v %v", book, err)
	}
	recent, err := repo.Trades(ctx, m, 2)
	if err != nil || len(recent) != 2 || recent[0].MatchSequence != 5 {
		t.Fatalf("trade order: %+v %v", recent, err)
	}
	ticker, err := repo.Ticker(ctx, m, start.Add(4*time.Minute))
	if err != nil || ticker.GetLastPriceUnits() != 103 {
		t.Fatalf("ticker: %+v %v", ticker, err)
	}
	stats, err := repo.Stats(ctx, m, start.Add(4*time.Minute))
	if err != nil || stats.GetTradeCount() != 3 || stats.GetBaseVolumeAtoms() != "3000" {
		t.Fatalf("rolling stats: %+v %v", stats, err)
	}
	page, more, err := repo.Candles(ctx, m, md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, start, start.Add(4*time.Minute), "", 2)
	if err != nil || !more || len(page) != 2 {
		t.Fatalf("first page: %+v more=%v err=%v", page, more, err)
	}
	next, more, err := repo.Candles(ctx, m, md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, start, start.Add(4*time.Minute),
		postgres.NanosDecimal(page[len(page)-1].GetStartTime().AsTime()), 2)
	if err != nil || more || len(next) != 1 || next[0].GetOpenPriceUnits() != 103 {
		t.Fatalf("next page: %+v more=%v err=%v", next, more, err)
	}
}
