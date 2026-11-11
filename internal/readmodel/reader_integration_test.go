//go:build integration

package readmodel_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	marketv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matching "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/statistics"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPublicHistoryAtomicCrashReplayAndLateCandleRevision(t *testing.T) {
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires MARKET_DATA_TEST_DATABASE_URL with migrations through 000004")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	var exists sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('market_data.public_trades')::text`).Scan(&exists); err != nil || !exists.Valid {
		t.Fatalf("apply 000002_public_projections.sql before test: %v", err)
	}
	topic := fmt.Sprintf("history-integration-%d", time.Now().UnixNano())
	partition := recovery.Partition{Topic: topic, ID: 0}
	def := &md.MarketProjectionDefinition{SourceMarketId: 7, DefinitionVersion: 3, Market: &marketv1.Market{
		MarketId: "BTC-USDT", PriceTickUnits: 25, QuantityStepBaseAtoms: 1000, BaseAssetScale: 8, QuoteAssetScale: 6, PriceScale: 2,
	}}
	store, err := postgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	w, err := recovery.New(topic, store, []recovery.MarketConfig{{PartitionID: 0, Depth: 5, Definition: def}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Assign(ctx, partition); err != nil {
		t.Fatal(err)
	}
	recorded := time.Date(2026, 11, 10, 12, 0, 50, 123456789, time.UTC)
	makeRecord := func(offset int64, seq uint64, orderID uint64, side orderv1.OrderSide, execution bool, stamp time.Time) recovery.Record {
		result := &matching.MatchingResult{MarketSequence: seq, Command: &matching.MatchingCommand{
			MarketId: 7, CommandId: fmt.Sprintf("p-%d", seq), Command: &matching.MatchingCommand_PlaceLimitOrder{PlaceLimitOrder: &matching.PlaceLimitOrder{
				OrderId: orderID, Side: side, PriceTicks: 100, QuantityLots: 1,
			}},
		}}
		if execution {
			result.Events = []*matching.SequencedMatchingEvent{{EventId: &matching.MatchingEventId{
				MarketId: 7, MarketSequence: seq, EventOrdinal: 0},
				Event: &matching.SequencedMatchingEvent_Exectuion{Exectuion: &matching.Execution{
					MakerOrderId: 1, TakerOrderId: orderID, TakerSide: side, PriceTicks: 100, QuantityLots: 1,
				}},
			}}
		}
		b, err := proto.Marshal(&matching.CommittedMatchingResult{Result: result, RecordedAt: timestamppb.New(stamp), MarketDefinitionVersion: 3})
		if err != nil {
			t.Fatal(err)
		}
		return recovery.Record{Partition: partition, Offset: offset, Key: []byte("7"), Value: b}
	}
	// Two SELL lots, two subsequent BUY takers each execute one lot.
	// Start with 2-lot ask so both executions are legal.
	a := makeRecord(0, 1, 1, orderv1.OrderSide_ORDER_SIDE_SELL, false, recorded)
	env := new(matching.CommittedMatchingResult)
	if err := proto.Unmarshal(a.Value, env); err != nil {
		t.Fatal(err)
	}
	env.Result.Command.GetPlaceLimitOrder().QuantityLots = 2
	a.Value, err = proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Process(ctx, a); err != nil {
		t.Fatal(err)
	}
	b := makeRecord(1, 2, 2, orderv1.OrderSide_ORDER_SIDE_BUY, true, recorded)
	if _, err := w.Process(ctx, b); err != nil {
		t.Fatal(err)
	}
	reader := &readmodel.Reader{DB: db, Topic: topic}
	view, err := reader.Load(ctx, 7, recorded.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if view.Snapshot.GetMarketSequence() != 2 || len(view.LatestTrades) != 1 || view.Stats.GetTradeCount() != 1 || len(view.Candles[md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE]) != 1 {
		t.Fatalf("incomplete atomic public projection: %+v", view)
	}
	if view.LatestTrades[0].GetQuoteQuantityAtoms() != 250 || view.Snapshot.GetAsks()[0].GetBaseQuantityAtoms() != "1000" {
		t.Fatal("lost exact amounts or remaining maker quantity")
	}
	retry := b
	retry.Offset = 2
	d, err := w.Process(ctx, retry)
	if err != nil || d != nil {
		t.Fatalf("duplicate was not idempotent: %v %v", d, err)
	}
	c := makeRecord(3, 3, 3, orderv1.OrderSide_ORDER_SIDE_BUY, true, recorded.Add(-30*time.Second))
	if _, err := w.Process(ctx, c); err != nil {
		t.Fatal(err)
	}
	updated, err := reader.Load(ctx, 7, recorded.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if updated.Snapshot.GetMarketSequence() != 3 || len(updated.LatestTrades) != 2 || len(updated.Snapshot.GetAsks()) != 0 {
		t.Fatal("full fill did not remove maker, or trades were lost")
	}
	candle := updated.Candles[md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE][0]
	if candle.GetRevision() != 1 || candle.GetTradeCount() != 2 || candle.GetBaseVolumeAtoms() != "2000" {
		t.Fatalf("late-recorded trade did not revise candle once: %+v", candle)
	}
	if updated.Stats.GetBaseVolumeAtoms() != "2000" || updated.Stats.GetQuoteVolumeAtoms() != "500" {
		t.Fatal("rolling totals inaccurate")
	}
	// CEX25.1: full-trade oracle parity for all materialized intervals.
	// A result inserted at a later Kafka offset has an earlier recorded time,
	// but the candle open/close must remain ordered by matching sequence.
	for _, interval := range statistics.SupportedIntervals() {
		begin, _, err := statistics.Window(recorded, interval)
		if err != nil {
			t.Fatal(err)
		}
		var encoded []byte
		err = db.QueryRowContext(ctx, `SELECT payload FROM market_data.candles
		    WHERE topic=$1 AND source_market_id=7 AND interval_id=$2 AND start_ns=$3`,
			topic, int32(interval), postgres.NanosDecimal(begin)).Scan(&encoded)
		if err != nil {
			t.Fatal(err)
		}
		persisted := new(md.Candle)
		if err := proto.Unmarshal(encoded, persisted); err != nil {
			t.Fatal(err)
		}
		expected, err := statistics.Candle("BTC-USDT", interval, begin, updated.LatestTrades, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(persisted, expected) {
			t.Fatalf("%s rollup materialization mismatch: got %v want %v", interval, persisted, expected)
		}
	}
	var rollupCount, rollupBase, rollupQuote string
	if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(trade_count),0)::text,coalesce(sum(base_volume_atoms),0)::text,
	     coalesce(sum(quote_volume_atoms),0)::text FROM market_data.trade_minute_rollups
	     WHERE topic=$1 AND source_market_id=7`, topic).Scan(&rollupCount, &rollupBase, &rollupQuote); err != nil ||
		rollupCount != "2" || rollupBase != "2000" || rollupQuote != "500" {
		t.Fatalf("atomic minute rollup not replay-safe: count=%s base=%s quote=%s err=%v", rollupCount, rollupBase, rollupQuote, err)
	}
	var durable int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM market_data.public_trades WHERE topic=$1 AND source_market_id=7`, topic).Scan(&durable); err != nil || durable != 2 {
		t.Fatalf("dedupe failed count=%d err=%v", durable, err)
	}
	if err := w.Revoke(ctx, partition); err != nil {
		t.Fatal(err)
	}
	w2, err := recovery.New(topic, store, []recovery.MarketConfig{{PartitionID: 0, Depth: 5, Definition: def}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Assign(ctx, partition); err != nil {
		t.Fatal(err)
	}
	if d, err := w2.Process(ctx, retry); err != nil || d != nil {
		t.Fatalf("replay advanced after restart: %v %v", d, err)
	}
	// Replaying an already committed Kafka offset must not revise candles or
	// duplicate the append-only candle update journal.
	var updates, candles int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM market_data.candle_updates WHERE topic=$1 AND source_market_id=7`, topic).Scan(&updates); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM market_data.candles WHERE topic=$1 AND source_market_id=7 AND revision=1`, topic).Scan(&candles); err != nil {
		t.Fatal(err)
	}
	if updates != 10 || candles != 5 {
		t.Fatalf("replayed duplicate mutated history: updates=%d revised_candles=%d", updates, candles)
	}
}
