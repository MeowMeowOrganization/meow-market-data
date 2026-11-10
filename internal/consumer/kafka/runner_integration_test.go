//go:build integration

package kafka

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	marketv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	marketdatav1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matchingv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Run against an EMPTY, single-partition, append-only topic created with the
// Docker command in README. This checks actual Kafka/PG binary protobuf IO,
// durable cursor advances, shutdown and consumer group restart/replay.
func TestKafkaRecoveryWithRealPostgres(t *testing.T) {
	brokers := os.Getenv("MARKET_DATA_TEST_KAFKA_BROKERS")
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	topic := os.Getenv("MARKET_DATA_TEST_KAFKA_TOPIC")
	if brokers == "" || dsn == "" || topic == "" {
		t.Skip("requires MARKET_DATA_TEST_KAFKA_BROKERS, MARKET_DATA_TEST_DATABASE_URL, MARKET_DATA_TEST_KAFKA_TOPIC")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Apply migrations/000001_recovery.sql using psql BEFORE this test.
	// pgx extended-protocol Exec does not accept multiple DDL statements.
	var exists sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT to_regclass('market_data.partition_cursors')::text").Scan(&exists); err != nil || !exists.Valid {
		t.Fatalf("apply migrations/000001_recovery.sql before integration tests: %v", err)
	}
	// Dedicated fresh topic is mandatory; do not run against a production topic.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM market_data.partition_cursors WHERE topic=$1`, topic).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("test topic %s was already checkpointed; create a fresh test topic", topic)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	def := &marketdatav1.MarketProjectionDefinition{
		SourceMarketId: 7, DefinitionVersion: 3,
		Market: &marketv1.Market{MarketId: "BTC-USDT", PriceTickUnits: 25, QuantityStepBaseAtoms: 1000},
	}
	store, _ := postgres.New(db)
	registry := []recovery.MarketConfig{{PartitionID: 0, Depth: 5, Definition: def}}
	cfg := Config{Brokers: strings.Split(brokers, ","), Topic: topic, Group: "md-e2e-" + strconv.FormatInt(time.Now().UnixNano(), 10)}
	start := func() (context.CancelFunc, <-chan error) {
		w, err := recovery.New(topic, store, registry)
		if err != nil {
			t.Fatal(err)
		}
		consumeCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- Run(consumeCtx, cfg, w) }()
		return stop, done
	}
	produce := func(seq uint64) {
		msg := &matchingv1.CommittedMatchingResult{
			Result: &matchingv1.MatchingResult{MarketSequence: seq,
				Command: &matchingv1.MatchingCommand{MarketId: 7, CommandId: fmt.Sprintf("p-%d", seq),
					Command: &matchingv1.MatchingCommand_PlaceLimitOrder{PlaceLimitOrder: &matchingv1.PlaceLimitOrder{
						OrderId: seq, Side: orderv1.OrderSide_ORDER_SIDE_BUY, PriceTicks: int64(100 + seq), QuantityLots: 1,
					}}}},
			RecordedAt: timestamppb.New(time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)), MarketDefinitionVersion: 3,
		}
		value, err := proto.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		results := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Key: []byte("7"), Value: value})
		if err := results.FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	wait := func(offset int64, done <-chan error) {
		t.Helper()
		timer := time.NewTicker(100 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case err := <-done:
				t.Fatalf("consumer exited before offset %d: %v", offset, err)
			case <-ctx.Done():
				t.Fatalf("timed out waiting for checkpoint %d: %v", offset, ctx.Err())
			case <-timer.C:
				var got int64
				err := db.QueryRowContext(ctx, `SELECT next_offset FROM market_data.partition_cursors WHERE topic=$1 AND partition_id=0`, topic).Scan(&got)
				if err == nil && got >= offset {
					return
				}
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					t.Fatal(err)
				}
			}
		}
	}
	stop, done := start()
	produce(1)
	produce(2)
	wait(2, done)
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown: %v", err)
	}
	stop, done = start()
	produce(3)
	wait(3, done)
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown: %v", err)
	}
}
