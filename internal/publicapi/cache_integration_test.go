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
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	rediscache "github.com/MeowMeowOrganization/meow-market-data/internal/storage/redis"
	_ "github.com/jackc/pgx/v5/stdlib"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCorruptRedisBookFallsBackToDurablePostgres(t *testing.T) {
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	url := os.Getenv("MARKET_DATA_TEST_REDIS_URL")
	if dsn == "" || url == "" {
		t.Skip("requires PostgreSQL and Redis integration endpoints")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	topic := fmt.Sprintf("cache-fallback-%d", time.Now().UnixNano())
	id := uint32(time.Now().UnixNano())
	market := marketregistry.Market{SourceID: id, Symbol: "BTC-USDT", DefinitionVersion: 3, Depth: 20, PriceTickUnits: 25, QuantityStepAtoms: 1000}
	bid, ask := uint64(10000), uint64(10100)
	at := time.Now().UTC()
	book := &md.BookSnapshot{MarketId: market.Symbol, SourceMarketId: id, MarketDefinitionVersion: 3, MarketSequence: 5, Depth: 20,
		Bids: []*md.BookLevel{{PriceUnits: bid, BaseQuantityAtoms: "2000"}}, Asks: []*md.BookLevel{{PriceUnits: ask, BaseQuantityAtoms: "1000"}}}
	raw, err := proto.Marshal(book)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO market_data.public_books
        (topic,source_market_id,market_id,market_sequence,market_definition_version,depth,recorded_at_ns,snapshot)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, topic, int64(id), market.Symbol, "5", "3", 20, postgres.NanosDecimal(at), raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2`, topic, int64(id))
	})
	cache, err := rediscache.New(url, topic, "1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	prefix := cache.Namespace.MarketPrefix(id, 3)
	// A separate connection handles fixture cleanup after we intentionally
	// close the production cache client to test PostgreSQL fallback.
	opts, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	cleaner := goredis.NewClient(opts)
	t.Cleanup(func() {
		_ = cleaner.Del(context.Background(), prefix+"state", prefix+"book:top", prefix+"ticker", prefix+"trades:latest", prefix+"stats:24h", prefix+"candles:1m", prefix+"candles:5m", prefix+"candles:15m", prefix+"candles:1h", prefix+"candles:1d", prefix+"book:live:state", prefix+"book:live").Err()
		_ = cleaner.Close()
	})
	b := &readmodel.Bundle{SourceMarketID: id, AsOf: at, Snapshot: book,
		Ticker: &md.Ticker{MarketId: market.Symbol, MarketSequence: 5, BestBidPriceUnits: &bid, BestAskPriceUnits: &ask, UpdatedAt: timestamppb.New(at)},
		Stats:  &md.Stats24H{MarketId: market.Symbol, WindowStartExclusive: timestamppb.New(at.Add(-24 * time.Hour)), AsOfInclusive: timestamppb.New(at), BaseVolumeAtoms: "0", QuoteVolumeAtoms: "0"}}
	if result, err := cache.Publish(ctx, b); err != nil || result != rediscache.PublishApplied {
		t.Fatalf("redis publish: %v %v", result, err)
	}
	repo := &Repository{DB: db, Topic: topic, Cache: cache.Client, CacheNamespace: cache.Namespace}
	if result, err := cache.SeedLiveBook(ctx, market, book, at); err != nil || result != rediscache.LiveApplied {
		t.Fatalf("seed live: %d %v", result, err)
	}
	got, err := repo.Book(ctx, market, 20)
	if err != nil || !proto.Equal(got, book) {
		t.Fatalf("live projection invalid: %v %v", got, err)
	}
	// Malformed live payload never leaks through the REST fast path.
	if err := cache.Client.Set(ctx, prefix+"book:live", `{"marketId":"BTC-USDT","sourceMarketId":0}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	got, err = repo.Book(ctx, market, 20)
	if err != nil || !proto.Equal(got, book) {
		t.Fatalf("corrupt cache must fall back: %+v %v", got, err)
	}
	// Even syntactically valid but crossed/negative order books must not be served.
	invalid := proto.Clone(book).(*md.BookSnapshot)
	invalid.Asks[0].PriceUnits = bid
	invalid.Asks[0].BaseQuantityAtoms = "-1000"
	value, err := protojson.Marshal(invalid)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Client.Set(ctx, prefix+"book:live", string(value), 0).Err(); err != nil {
		t.Fatal(err)
	}
	got, err = repo.Book(ctx, market, 20)
	if err != nil || !proto.Equal(got, book) {
		t.Fatalf("invalid levels must fall back: %+v %v", got, err)
	}
	// A disconnected Redis client is not a dependency of durable book serving.
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = repo.Book(ctx, market, 20)
	if err != nil || !proto.Equal(got, book) {
		t.Fatalf("Redis outage must fall back: %+v %v", got, err)
	}
}
