package livebook

import (
	"context"
	"errors"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	rc "github.com/MeowMeowOrganization/meow-market-data/internal/storage/redis"
)

type interruptedCache struct {
	*memoryCache
	down bool
}

func (c *interruptedCache) Load(ctx context.Context, m marketregistry.Market, at time.Time) (*rc.LiveBook, error) {
	if c.down {
		return nil, errors.New("simulated redis connection failure")
	}
	return c.memoryCache.Load(ctx, m, at)
}
func (c *interruptedCache) Seed(ctx context.Context, m marketregistry.Market, s *md.BookSnapshot, at time.Time) (rc.LiveResult, error) {
	if c.down {
		return 0, errors.New("simulated redis connection failure")
	}
	return c.memoryCache.Seed(ctx, m, s, at)
}
func (c *interruptedCache) Advance(ctx context.Context, m marketregistry.Market, p *rc.LiveBook, s *md.BookSnapshot, at time.Time, caughtUp bool) (rc.LiveResult, error) {
	if c.down {
		return 0, errors.New("simulated redis connection failure")
	}
	return c.memoryCache.Advance(ctx, m, p, s, at, caughtUp)
}

func TestRedisConnectionLossDoesNotAdvanceAndRecoveryCatchesUp(t *testing.T) {
	now := time.Now().UTC()
	j := &memoryJournal{snapshot: testSnapshot(3), head: 3, deltas: []*md.BookDelta{testDelta(2), testDelta(3)}}
	c := &interruptedCache{memoryCache: &memoryCache{live: record(testSnapshot(1), now)}, down: true}
	w := &Worker{Journal: j, Cache: c}
	if err := w.Step(context.Background(), testMarket, now); err == nil || c.live.Sequence != 1 || c.seedCalls != 0 || c.advanceCalls != 0 {
		t.Fatalf("Redis outage changed projection: %v cache=%+v", err, c.memoryCache)
	}
	// Kafka/PG continued to advance independently while Redis was offline.
	c.down = false
	if err := w.Step(context.Background(), testMarket, now); err != nil {
		t.Fatal(err)
	}
	if c.live.Sequence != 3 || c.advanceCalls != 2 || c.seedCalls != 0 {
		t.Fatalf("journal did not catch up after Redis recovery: %+v", c.memoryCache)
	}
	// Complete eviction after service restart must seed from PG without
	// needing a new Kafka event or reprocessing any financial side effect.
	c.live = nil
	if err := w.Step(context.Background(), testMarket, now); err != nil {
		t.Fatal(err)
	}
	if c.live.Sequence != 3 || c.seedCalls != 1 {
		t.Fatalf("cold recovery failed: %+v", c.memoryCache)
	}
}
