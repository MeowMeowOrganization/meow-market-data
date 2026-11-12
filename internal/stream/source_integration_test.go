//go:build integration

package stream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	marketv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matching "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDurableBookTradesCandleJournalAndReplayFloor(t *testing.T) {
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set MARKET_DATA_TEST_DATABASE_URL; apply migrations 000001, 000002, 000003")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	var exists sql.NullString
	if err = db.QueryRowContext(ctx, `SELECT to_regclass('market_data.candle_updates')::text`).Scan(&exists); err != nil || !exists.Valid {
		t.Fatalf("migration 000003 required: %v", err)
	}
	topic := fmt.Sprintf("stream-e2e-%d", time.Now().UnixNano())
	m := marketregistry.Market{Symbol: "BTC-USDT", SourceID: 7, DefinitionVersion: 3, Depth: 5}
	def := &md.MarketProjectionDefinition{SourceMarketId: 7, DefinitionVersion: 3, Market: &marketv1.Market{
		MarketId: m.Symbol, PriceTickUnits: 25, QuantityStepBaseAtoms: 1000, BaseAssetScale: 8, QuoteAssetScale: 6, PriceScale: 2,
	}}
	store, err := postgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := recovery.New(topic, store, []recovery.MarketConfig{{PartitionID: 0, Depth: m.Depth, Definition: def}})
	if err != nil {
		t.Fatal(err)
	}
	part := recovery.Partition{Topic: topic, ID: 0}
	if err = worker.Assign(ctx, part); err != nil {
		t.Fatal(err)
	}
	timestamp := time.Date(2026, 11, 10, 12, 0, 45, 123456789, time.UTC)
	makeInput := func(seq, orderID uint64, side orderv1.OrderSide, execute bool) recovery.Record {
		command := &matching.MatchingCommand{MarketId: 7, CommandId: fmt.Sprintf("cmd-%d", seq),
			Command: &matching.MatchingCommand_PlaceLimitOrder{PlaceLimitOrder: &matching.PlaceLimitOrder{
				OrderId: orderID, Side: side, PriceTicks: 100, QuantityLots: 1,
			}}}
		result := &matching.MatchingResult{MarketSequence: seq, Command: command}
		if execute {
			result.Events = []*matching.SequencedMatchingEvent{{EventId: &matching.MatchingEventId{
				MarketId: 7, MarketSequence: seq, EventOrdinal: 0,
			}, Event: &matching.SequencedMatchingEvent_Exectuion{Exectuion: &matching.Execution{
				MakerOrderId: 1, TakerOrderId: orderID, TakerSide: side, PriceTicks: 100, QuantityLots: 1,
			}}}}
		}
		bytes, err := proto.Marshal(&matching.CommittedMatchingResult{Result: result, RecordedAt: timestamppb.New(timestamp), MarketDefinitionVersion: 3})
		if err != nil {
			t.Fatal(err)
		}
		return recovery.Record{Partition: part, Offset: int64(seq - 1), Key: []byte("7"), Value: bytes}
	}
	if _, err = worker.Process(ctx, makeInput(1, 1, orderv1.OrderSide_ORDER_SIDE_SELL, false)); err != nil {
		t.Fatal(err)
	}
	// Before the first execution the ticker has NO last price. The resting
	// ask must still be read from the coherent durable book snapshot.
	source := &PostgresSource{DB: db, Topic: topic}
	before, err := source.LatestTicker(ctx, m)
	if err != nil || before.GetMarketSequence() != 1 || before.LastPriceUnits != nil ||
		before.BestAskPriceUnits == nil || before.GetUpdatedAt() == nil {
		t.Fatalf("pre-trade durable ticker: %+v %v", before, err)
	}
	if _, err = worker.Process(ctx, makeInput(2, 2, orderv1.OrderSide_ORDER_SIDE_BUY, true)); err != nil {
		t.Fatal(err)
	}
	current, err := source.LatestTicker(ctx, m)
	if err != nil || current.GetMarketSequence() != 2 || current.GetLastPriceUnits() != 2500 ||
		!current.GetUpdatedAt().AsTime().Equal(timestamp) {
		t.Fatalf("committed trade ticker must be coherent: %+v %v", current, err)
	}
	snap, err := source.Snapshot(ctx, m)
	if err != nil || snap.GetMarketSequence() != 2 {
		t.Fatalf("snapshot: %+v %v", snap, err)
	}
	floor, err := source.ReplayFloor(ctx, m)
	if err != nil || floor != 1 {
		t.Fatalf("floor %d %v", floor, err)
	}
	results, err := source.Results(ctx, m, 0, 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Delta.GetMarketSequence() != 1 || results[1].Delta.GetMarketSequence() != 2 ||
		len(results[1].Trades) != 1 || len(results[1].Candles) != 5 {
		t.Fatalf("missing durable stream events: %+v", results)
	}
	if results[1].Trades[0].GetTradeId() == "" {
		t.Fatal("lost canonical trade identity")
	}
	if _, err = db.ExecContext(ctx, `DELETE FROM market_data.book_deltas WHERE topic=$1 AND source_market_id=7 AND market_sequence=1`, topic); err != nil {
		t.Fatal(err)
	}
	_, err = source.Results(ctx, m, 0, 2, 10)
	if !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("expected fail-closed gap, got %v", err)
	}
}
