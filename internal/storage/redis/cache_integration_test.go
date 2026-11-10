//go:build integration

package redis_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	cachepkg "github.com/MeowMeowOrganization/meow-market-data/internal/storage/redis"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRedisAtomicReplaceFencesStaleAndRebuilds(t *testing.T) {
	url := os.Getenv("MARKET_DATA_TEST_REDIS_URL")
	if url == "" {
		t.Skip("set MARKET_DATA_TEST_REDIS_URL")
	}
	cache, err := cachepkg.New(url)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := uint32(time.Now().UnixNano())
	at := time.Now().UTC()
	b := &readmodel.Bundle{SourceMarketID: id, AsOf: at,
		Snapshot: &md.BookSnapshot{SourceMarketId: id, MarketId: "TEST-USDT", MarketSequence: 9007199254740993, Depth: 5},
		Ticker:   &md.Ticker{MarketId: "TEST-USDT", MarketSequence: 9007199254740993, UpdatedAt: timestamppb.New(at)},
		Stats:    &md.Stats24H{MarketId: "TEST-USDT", WindowStartExclusive: timestamppb.New(at.Add(-24 * time.Hour)), AsOfInclusive: timestamppb.New(at), BaseVolumeAtoms: "0", QuoteVolumeAtoms: "0"},
	}
	prefix := fmt.Sprintf("md:v1:m:{%d}:", id)
	defer cache.Client.Del(context.Background(), prefix+"state", prefix+"book:top", prefix+"ticker", prefix+"trades:latest", prefix+"stats:24h",
		prefix+"candles:1m", prefix+"candles:5m", prefix+"candles:15m", prefix+"candles:1h", prefix+"candles:1d")
	if err := cache.Publish(ctx, b); err != nil {
		t.Fatal(err)
	}
	old := *b
	old.Snapshot = &md.BookSnapshot{SourceMarketId: id, MarketId: "TEST-USDT", MarketSequence: 9007199254740992, Depth: 5}
	old.Ticker = &md.Ticker{MarketId: "TEST-USDT", MarketSequence: 9007199254740992}
	if err := cache.Publish(ctx, &old); err != nil {
		t.Fatal(err)
	}
	got, err := cache.Client.HGet(ctx, prefix+"state", "sequence").Result()
	if err != nil || got != "9007199254740993" {
		t.Fatalf("uint64 fence failed: %s %v", got, err)
	}
	if _, err := cache.Client.Get(ctx, prefix+"book:top").Result(); err != nil {
		t.Fatal("missing book", err)
	}
	// A complete cache eviction must be repairable without new Kafka records.
	if err := cache.Client.Del(ctx, prefix+"state", prefix+"book:top").Err(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Publish(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Client.Get(ctx, prefix+"book:top").Result(); err != nil {
		t.Fatal("cache did not rebuild", err)
	}
}
