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
	"google.golang.org/protobuf/encoding/protojson"
)

func integrationCache(t *testing.T, topic, epoch string) *Cache {
	t.Helper()
	url := os.Getenv("MARKET_DATA_TEST_REDIS_URL")
	if url == "" {
		t.Skip("set MARKET_DATA_TEST_REDIS_URL")
	}
	cache, err := New(url, topic, epoch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}
func integrationKeys(prefix string) []string {
	keys := []string{"state", "book:top", "ticker", "trades:latest", "stats:24h", "candles:1m", "candles:5m", "candles:15m", "candles:1h", "candles:1d"}
	for i := range keys {
		keys[i] = prefix + keys[i]
	}
	return keys
}
func TestRedisFencesStaleAndRebuilds(t *testing.T) {
	id := uint32(time.Now().UnixNano())
	cache := integrationCache(t, fmt.Sprintf("redis-test.%d", time.Now().UnixNano()), "1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := cache.Namespace.MarketPrefix(id, 3)
	t.Cleanup(func() { _ = cache.Client.Del(context.Background(), integrationKeys(prefix)...).Err() })
	at := time.Now().UTC()
	const high uint64 = 9007199254740993
	b := sampleBundle(id, high, at)
	if result, err := cache.Publish(ctx, b); err != nil || result != PublishApplied {
		t.Fatalf("publish: %v %v", result, err)
	}
	old := sampleBundle(id, high-1, at)
	if result, err := cache.Publish(ctx, old); err != nil || result != PublishStale {
		t.Fatalf("stale must be observable: %v %v", result, err)
	}
	olderRefresh := sampleBundle(id, high, at.Add(-time.Second))
	if result, err := cache.Publish(ctx, olderRefresh); err != nil || result != PublishStale {
		t.Fatalf("stale as-of must be observable: %v %v", result, err)
	}
	view, err := ReadFresh(ctx, cache.Client, cache.Namespace, id, 3, time.Now().UTC(), "book:top", "ticker")
	if err != nil || view.Sequence != high {
		t.Fatalf("uint64 sequence fence failed: %+v %v", view, err)
	}
	// A partial eviction makes the complete view unavailable, not half-valid.
	if err := cache.Client.Del(ctx, prefix+"book:top").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFresh(ctx, cache.Client, cache.Namespace, id, 3, time.Now().UTC(), "book:top", "ticker"); !errors.Is(err, ErrUnavailableView) {
		t.Fatalf("partial eviction: %v", err)
	}
	if result, err := cache.Publish(ctx, b); err != nil || result != PublishApplied {
		t.Fatalf("repair: %v %v", result, err)
	}
	// A corrupted, far-future timestamp cannot make same-sequence repair impossible.
	future, _ := padDecimal(strconv.FormatInt(time.Now().Add(10*time.Minute).UnixNano(), 10), 30)
	if err := cache.Client.HSet(ctx, prefix+"state", "asof_sort", future).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFresh(ctx, cache.Client, cache.Namespace, id, 3, time.Now().UTC(), "book:top"); !errors.Is(err, ErrUnavailableView) {
		t.Fatalf("future timestamp should fail closed: %v", err)
	}
	if result, err := cache.Publish(ctx, b); err != nil || result != PublishApplied {
		t.Fatalf("future timestamp repair: %v %v", result, err)
	}
	if _, err := ReadFresh(ctx, cache.Client, cache.Namespace, id, 3, time.Now().UTC(), "book:top"); err != nil {
		t.Fatalf("rebuild unavailable: %v", err)
	}
}

func TestRedisEpochResetAndMarketVersionIsolation(t *testing.T) {
	id := uint32(time.Now().UnixNano())
	topic := fmt.Sprintf("epoch-test.%d", time.Now().UnixNano())
	before := integrationCache(t, topic, "1")
	after := integrationCache(t, topic, "2")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		_ = before.Client.Del(context.Background(), integrationKeys(before.Namespace.MarketPrefix(id, 3))...).Err()
		_ = after.Client.Del(context.Background(), integrationKeys(after.Namespace.MarketPrefix(id, 3))...).Err()
	})
	at := time.Now().UTC()
	if result, err := before.Publish(ctx, sampleBundle(id, 50000, at)); err != nil || result != PublishApplied {
		t.Fatalf("old source: %v %v", result, err)
	}
	if result, err := after.Publish(ctx, sampleBundle(id, 1, at)); err != nil || result != PublishApplied {
		t.Fatalf("new generation: %v %v", result, err)
	}
	for _, test := range []struct {
		cache *Cache
		want  uint64
	}{{before, 50000}, {after, 1}} {
		view, err := ReadFresh(ctx, test.cache.Client, test.cache.Namespace, id, 3, time.Now().UTC(), "book:top")
		if err != nil || view.Sequence != test.want {
			t.Fatalf("epoch isolation: %+v %v", view, err)
		}
	}
	if _, err := ReadFresh(ctx, after.Client, after.Namespace, id, 4, time.Now().UTC(), "book:top"); !errors.Is(err, ErrUnavailableView) {
		t.Fatalf("market version must not read another definition: %v", err)
	}
}

func TestRedisConcurrentPublicationAndAtomicReaders(t *testing.T) {
	id := uint32(time.Now().UnixNano())
	cache := integrationCache(t, fmt.Sprintf("concurrent-test.%d", time.Now().UnixNano()), "1")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := cache.Namespace.MarketPrefix(id, 3)
	t.Cleanup(func() { _ = cache.Client.Del(context.Background(), integrationKeys(prefix)...).Err() })
	at := time.Now().UTC()
	if result, err := cache.Publish(ctx, sampleBundle(id, 100, at)); err != nil || result != PublishApplied {
		t.Fatalf("first publish: %v %v", result, err)
	}
	var wg sync.WaitGroup
	errorsCh := make(chan error, 200)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(101); i <= 140; i++ {
			_, err := cache.Publish(ctx, sampleBundle(id, i, at))
			if err != nil {
				errorsCh <- err
				return
			}
		}
	}()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 60; j++ {
				view, err := ReadFresh(ctx, cache.Client, cache.Namespace, id, 3, time.Now().UTC(), "book:top", "ticker")
				if err != nil {
					errorsCh <- err
					return
				}
				book, ticker := new(md.BookSnapshot), new(md.Ticker)
				if protojson.Unmarshal([]byte(view.Values[0]), book) != nil || protojson.Unmarshal([]byte(view.Values[1]), ticker) != nil ||
					book.GetMarketSequence() != view.Sequence || ticker.GetMarketSequence() != view.Sequence {
					errorsCh <- errors.New("mixed book/ticker sequences observed")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	view, err := ReadFresh(ctx, cache.Client, cache.Namespace, id, 3, time.Now().UTC(), "book:top")
	if err != nil || view.Sequence != 140 {
		t.Fatalf("expected final sequence 140, got %+v %v", view, err)
	}
}
