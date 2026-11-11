//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"google.golang.org/protobuf/proto"
)

func liveTestMarket() marketregistry.Market {
	return marketregistry.Market{SourceID: uint32(time.Now().UnixNano()), Symbol: "BTC-USDT", DefinitionVersion: 3,
		Depth: 20, PriceTickUnits: 25, QuantityStepAtoms: 1000}
}
func liveFixture(m marketregistry.Market, seq uint64) *md.BookSnapshot {
	return &md.BookSnapshot{MarketId: m.Symbol, SourceMarketId: m.SourceID, MarketSequence: seq,
		MarketDefinitionVersion: m.DefinitionVersion, Depth: m.Depth,
		Bids: []*md.BookLevel{{PriceUnits: 10000, BaseQuantityAtoms: "2000"}},
		Asks: []*md.BookLevel{{PriceUnits: 10100, BaseQuantityAtoms: "3000"}}}
}
func TestLiveBookAtomicSequenceCASAndSourceFencing(t *testing.T) {
	cache := integrationCache(t, fmt.Sprintf("live-test.%d", time.Now().UnixNano()), "1")
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	m := liveTestMarket()
	keys := liveKeys(cache.Namespace, m)
	t.Cleanup(func() { _ = cache.Client.Del(context.Background(), keys...).Err() })
	const seq uint64 = 9007199254740993 // above 2^53; no Lua tonumber allowed
	at := time.Now().UTC()
	first := liveFixture(m, seq)
	result, err := cache.SeedLiveBook(ctx, m, first, at)
	if err != nil || result != LiveApplied {
		t.Fatalf("seed: %v %v", result, err)
	}
	old, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, at, true)
	if err != nil || old.Sequence != seq {
		t.Fatalf("atomic read: %+v %v", old, err)
	}
	next := proto.Clone(first).(*md.BookSnapshot)
	next.MarketSequence = seq + 1
	next.Bids[0].BaseQuantityAtoms = "3000"
	result, err = cache.AdvanceLiveBook(ctx, m, old, next, time.Now().UTC(), true)
	if err != nil || result != LiveApplied {
		t.Fatalf("advance: %v %v", result, err)
	}
	result, err = cache.AdvanceLiveBook(ctx, m, old, next, time.Now().UTC(), true)
	if err != nil || result != LiveRetry {
		t.Fatalf("duplicate CAS must retry: %v %v", result, err)
	}
	result, err = cache.SeedLiveBook(ctx, m, first, time.Now().UTC())
	if err != nil || result != LiveRetry {
		t.Fatalf("stale snapshot rewound Redis: %v %v", result, err)
	}
	different := proto.Clone(next).(*md.BookSnapshot)
	different.Bids[0].BaseQuantityAtoms = "4000"
	result, err = cache.SeedLiveBook(ctx, m, different, time.Now().UTC())
	if !errors.Is(err, ErrLiveConflict) || result != LiveConflict {
		t.Fatalf("same-sequence conflict not caught: %v %v", result, err)
	}
	loaded, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), true)
	if err != nil || !proto.Equal(loaded.Snapshot, next) {
		t.Fatalf("CAS altered book: %+v %v", loaded, err)
	}
}
func TestLiveBookRepairPartialEvictionAndCorruption(t *testing.T) {
	cache := integrationCache(t, fmt.Sprintf("repair-test.%d", time.Now().UnixNano()), "1")
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	m := liveTestMarket()
	keys := liveKeys(cache.Namespace, m)
	t.Cleanup(func() { _ = cache.Client.Del(context.Background(), keys...).Err() })
	at := time.Now().UTC()
	fixture := liveFixture(m, 55)
	if _, err := cache.SeedLiveBook(ctx, m, fixture, at); err != nil {
		t.Fatal(err)
	}
	if err := cache.Client.Del(ctx, keys[1]).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, at, true); !errors.Is(err, ErrUnavailableView) {
		t.Fatalf("partial eviction not rejected: %v", err)
	}
	if _, err := cache.SeedLiveBook(ctx, m, fixture, time.Now().UTC()); err != nil {
		t.Fatalf("partial eviction repair: %v", err)
	}
	if err := cache.Client.Set(ctx, keys[1], `{"broken":"payload"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, at, false)
	var corrupt *CorruptLiveBookError
	if !errors.As(err, &corrupt) {
		t.Fatalf("expected corrupt cache signal: %v", err)
	}
	result, err := cache.RepairCorruptLiveBook(ctx, m, corrupt, fixture, time.Now().UTC())
	if err != nil || result != LiveApplied {
		t.Fatalf("CAS repair: %v %v", result, err)
	}
	if _, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), true); err != nil {
		t.Fatal(err)
	}
	// Invalid future refresh cannot indefinitely block otherwise valid book.
	future, _ := padDecimal(strconv.FormatInt(time.Now().Add(time.Hour).UnixNano(), 10), 30)
	if err := cache.Client.HSet(ctx, keys[0], "asof_sort", future).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), true); !errors.Is(err, ErrUnavailableView) {
		t.Fatal("future-dated cache accepted")
	}
	valid, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.TouchLiveBook(ctx, m, valid, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), true); err != nil {
		t.Fatalf("future timestamp not repaired: %v", err)
	}
}
func TestLiveBookQuarantineIsCASFenced(t *testing.T) {
	cache := integrationCache(t, fmt.Sprintf("quarantine-test.%d", time.Now().UnixNano()), "1")
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	m := liveTestMarket()
	keys := liveKeys(cache.Namespace, m)
	t.Cleanup(func() { _ = cache.Client.Del(context.Background(), keys...).Err() })
	first := liveFixture(m, 10)
	if _, err := cache.SeedLiveBook(ctx, m, first, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	prev, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), true)
	if err != nil {
		t.Fatal(err)
	}
	next := proto.Clone(first).(*md.BookSnapshot)
	next.MarketSequence = 11
	if _, err := cache.AdvanceLiveBook(ctx, m, prev, next, time.Now().UTC(), true); err != nil {
		t.Fatal(err)
	}
	status, err := cache.QuarantineLiveBook(ctx, m, prev)
	if err != nil || status != LiveRetry {
		t.Fatalf("late quarantine overwrote newer book: %v %v", status, err)
	}
	current, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), true)
	if err != nil {
		t.Fatal(err)
	}
	status, err = cache.QuarantineLiveBook(ctx, m, current)
	if err != nil || status != LiveApplied {
		t.Fatalf("quarantine failed: %v %v", status, err)
	}
	if _, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), true); !errors.Is(err, ErrUnavailableView) {
		t.Fatalf("quarantined book still served: %v", err)
	}
}

func TestLiveBookConcurrentWritersExactlyOneWins(t *testing.T) {
	cache := integrationCache(t, fmt.Sprintf("race-test.%d", time.Now().UnixNano()), "1")
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	m := liveTestMarket()
	keys := liveKeys(cache.Namespace, m)
	t.Cleanup(func() { _ = cache.Client.Del(context.Background(), keys...).Err() })
	first := liveFixture(m, 100)
	if _, err := cache.SeedLiveBook(ctx, m, first, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	before, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), true)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan LiveResult, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			next := proto.Clone(first).(*md.BookSnapshot)
			next.MarketSequence = 101
			next.Bids[0].BaseQuantityAtoms = "5000"
			r, e := cache.AdvanceLiveBook(ctx, m, before, next, time.Now().UTC(), true)
			results <- r
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var success, retries int
	for result := range results {
		if result == LiveApplied {
			success++
		} else if result == LiveRetry {
			retries++
		}
	}
	if success != 1 || retries != 7 {
		t.Fatalf("CAS winners=%d losers=%d", success, retries)
	}
	loaded, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), true)
	if err != nil || loaded.Sequence != 101 || loaded.Snapshot.Bids[0].BaseQuantityAtoms != "5000" {
		t.Fatalf("concurrent writer inconsistency: %+v %v", loaded, err)
	}
}

// BenchmarkLiveBookReadCAS measures real Redis read + conditional publication
// with an advancing snapshot; run against isolated test Redis.
func BenchmarkLiveBookReadCAS(b *testing.B) {
	url := os.Getenv("MARKET_DATA_TEST_REDIS_URL")
	if url == "" {
		b.Skip("set MARKET_DATA_TEST_REDIS_URL")
	}
	cache, err := New(url, fmt.Sprintf("bench-live.%d", time.Now().UnixNano()), "1")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = cache.Close() })
	m := liveTestMarket()
	keys := liveKeys(cache.Namespace, m)
	b.Cleanup(func() { _ = cache.Client.Del(context.Background(), keys...).Err() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	first := liveFixture(m, 1)
	if _, err := cache.SeedLiveBook(ctx, m, first, time.Now().UTC()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prev, err := LoadLiveBook(ctx, cache.Client, cache.Namespace, m, time.Now().UTC(), false)
		if err != nil {
			b.Fatal(err)
		}
		next := proto.Clone(prev.Snapshot).(*md.BookSnapshot)
		next.MarketSequence++
		next.Bids[0].BaseQuantityAtoms = strconv.FormatUint(1000+uint64(i%100)*1000, 10)
		result, err := cache.AdvanceLiveBook(ctx, m, prev, next, time.Now().UTC(), true)
		if err != nil || result != LiveApplied {
			b.Fatalf("sequence CAS result=%d err=%v", result, err)
		}
	}
}
