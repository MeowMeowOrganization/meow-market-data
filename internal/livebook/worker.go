// Package livebook projects the committed PostgreSQL L2 journal into Redis
// without becoming part of source Kafka offset acknowledgement. Redis state
// is always reconstructable from the durable full snapshot and delta journal.
package livebook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/l2"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	rediscache "github.com/MeowMeowOrganization/meow-market-data/internal/storage/redis"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const BatchLimit = 128
const IdleTouchInterval = 3 * time.Second

type Cache interface {
	Load(context.Context, marketregistry.Market, time.Time) (*rediscache.LiveBook, error)
	Seed(context.Context, marketregistry.Market, *md.BookSnapshot, time.Time) (rediscache.LiveResult, error)
	Advance(context.Context, marketregistry.Market, *rediscache.LiveBook, *md.BookSnapshot, time.Time, bool) (rediscache.LiveResult, error)
	Touch(context.Context, marketregistry.Market, *rediscache.LiveBook, time.Time) (rediscache.LiveResult, error)
	Repair(context.Context, marketregistry.Market, *rediscache.CorruptLiveBookError, *md.BookSnapshot, time.Time) (rediscache.LiveResult, error)
	Quarantine(context.Context, marketregistry.Market, *rediscache.LiveBook) (rediscache.LiveResult, error)
}

// RedisAdapter separates storage methods from worker orchestration for tests.
type RedisAdapter struct{ Cache *rediscache.Cache }

func (a RedisAdapter) Load(ctx context.Context, m marketregistry.Market, now time.Time) (*rediscache.LiveBook, error) {
	return rediscache.LoadLiveBook(ctx, a.Cache.Client, a.Cache.Namespace, m, now, false)
}
func (a RedisAdapter) Seed(ctx context.Context, m marketregistry.Market, s *md.BookSnapshot, at time.Time) (rediscache.LiveResult, error) {
	return a.Cache.SeedLiveBook(ctx, m, s, at)
}
func (a RedisAdapter) Advance(ctx context.Context, m marketregistry.Market, prev *rediscache.LiveBook, next *md.BookSnapshot, at time.Time, caughtUp bool) (rediscache.LiveResult, error) {
	return a.Cache.AdvanceLiveBook(ctx, m, prev, next, at, caughtUp)
}
func (a RedisAdapter) Touch(ctx context.Context, m marketregistry.Market, live *rediscache.LiveBook, at time.Time) (rediscache.LiveResult, error) {
	return a.Cache.TouchLiveBook(ctx, m, live, at)
}

func (a RedisAdapter) Repair(ctx context.Context, m marketregistry.Market, corrupt *rediscache.CorruptLiveBookError, snap *md.BookSnapshot, at time.Time) (rediscache.LiveResult, error) {
	return a.Cache.RepairCorruptLiveBook(ctx, m, corrupt, snap, at)
}

func (a RedisAdapter) Quarantine(ctx context.Context, m marketregistry.Market, live *rediscache.LiveBook) (rediscache.LiveResult, error) {
	return a.Cache.QuarantineLiveBook(ctx, m, live)
}

type Worker struct {
	Journal Journal
	Cache   Cache
	Markets []marketregistry.Market
}

func (w *Worker) rebuild(ctx context.Context, m marketregistry.Market, now time.Time) error {
	snapshot, err := w.Journal.Snapshot(ctx, m)
	if errors.Is(err, readmodel.ErrNotReady) {
		return nil
	}
	if err != nil {
		return err
	}
	result, err := w.Cache.Seed(ctx, m, snapshot, now)
	if err != nil {
		return err
	}
	if result != rediscache.LiveApplied && result != rediscache.LiveRetry {
		return fmt.Errorf("livebook: unexpected seed result %d", result)
	}
	return nil
}

// repairCorrupt verifies a durable snapshot before a same-sequence CAS repair.
// A source regression is never "fixed" by rewinding Redis.
func (w *Worker) repairCorrupt(ctx context.Context, m marketregistry.Market, now time.Time, corrupt *rediscache.CorruptLiveBookError) error {
	snapshot, err := w.Journal.Snapshot(ctx, m)
	if err != nil {
		return err
	}
	if snapshot.GetMarketSequence() < corrupt.Sequence {
		return ErrSourceRegression
	}
	if snapshot.GetMarketSequence() > corrupt.Sequence {
		result, err := w.Cache.Seed(ctx, m, snapshot, now)
		if err != nil {
			return err
		}
		if result != rediscache.LiveApplied && result != rediscache.LiveRetry {
			return fmt.Errorf("livebook: unexpected repair seed %d", result)
		}
		return nil
	}
	result, err := w.Cache.Repair(ctx, m, corrupt, snapshot, now)
	if err != nil {
		return err
	}
	if result != rediscache.LiveApplied && result != rediscache.LiveRetry {
		return fmt.Errorf("livebook: unexpected repair result %d", result)
	}
	return nil
}

func (w *Worker) failClosed(ctx context.Context, m marketregistry.Market, live *rediscache.LiveBook, cause error) error {
	_, err := w.Cache.Quarantine(ctx, m, live)
	return errors.Join(cause, err)
}

// Step processes at most one bounded journal batch for a single market. It
// never advances Redis past a missing sequence. On journal gap or Redis loss,
// reconstruction jumps only to a validated durable PostgreSQL snapshot.
func (w *Worker) Step(ctx context.Context, m marketregistry.Market, now time.Time) error {
	if w == nil || w.Journal == nil || w.Cache == nil {
		return errors.New("livebook: missing dependencies")
	}
	live, err := w.Cache.Load(ctx, m, now)
	var corrupt *rediscache.CorruptLiveBookError
	if errors.As(err, &corrupt) {
		return w.repairCorrupt(ctx, m, now, corrupt)
	}
	if errors.Is(err, rediscache.ErrUnavailableView) {
		return w.rebuild(ctx, m, now)
	}
	if err != nil {
		return err
	}
	if live == nil || live.Snapshot == nil {
		return errors.New("livebook: nil live cache view")
	}
	head, deltas, err := w.Journal.Fetch(ctx, m, live.Sequence, BatchLimit)
	if errors.Is(err, ErrJournalGap) {
		if recoveryErr := w.rebuild(ctx, m, now); recoveryErr != nil {
			return w.failClosed(ctx, m, live, recoveryErr)
		}
		return nil
	}
	if errors.Is(err, readmodel.ErrNotReady) {
		return nil
	}
	if errors.Is(err, ErrSourceRegression) || errors.Is(err, ErrCorruptJournal) {
		return w.failClosed(ctx, m, live, err)
	}
	if err != nil {
		return err
	}
	if head < live.Sequence {
		return w.failClosed(ctx, m, live, ErrSourceRegression)
	}
	if len(deltas) == 0 {
		// A current-as-of timestamp means the worker successfully checked the
		// durable head; it must not be inferred from Redis alone.
		if live.AsOfSort != "" && now.After(time.Unix(0, 0).Add(IdleTouchInterval)) {
			threshold, err := rediscache.AsOfSort(now.Add(-IdleTouchInterval))
			if err != nil {
				return err
			}
			upper, err := rediscache.AsOfSort(now.Add(5 * time.Second))
			if err != nil {
				return err
			}
			if live.AsOfSort >= threshold && live.AsOfSort <= upper {
				return nil
			}
		}
		// Periodic parity audit guards against valid-looking but divergent
		// Redis state (e.g. a replica running incompatible projection code).
		// A newer durable head may appear after Fetch; never heartbeat that
		// older book as caught up until the next journal poll applies it.
		authoritative, err := w.Journal.Snapshot(ctx, m)
		if err != nil {
			return err
		}
		if authoritative.GetMarketSequence() != live.Sequence {
			return nil
		}
		if !proto.Equal(authoritative, live.Snapshot) {
			return w.failClosed(ctx, m, live, rediscache.ErrLiveConflict)
		}
		_, err = w.Cache.Touch(ctx, m, live, now)
		return err
	}
	for _, delta := range deltas {
		next, err := l2.Apply(live.Snapshot, delta, expected(m))
		if err != nil {
			return w.failClosed(ctx, m, live, err)
		} // invariant violation; do not silently resnapshot corrupt deltas
		// Certify freshness only after comparing the reconstructed head
		// against the authoritative full PG snapshot. A later commit can
		// race this read: in that case advance but keep the cache uncertified.
		caughtUp := false
		if delta.GetMarketSequence() == head {
			authoritative, err := w.Journal.Snapshot(ctx, m)
			if err != nil {
				return err
			}
			if authoritative.GetMarketSequence() == head {
				if !proto.Equal(authoritative, next) {
					return w.failClosed(ctx, m, live, rediscache.ErrLiveConflict)
				}
				caughtUp = true
			}
		}
		result, err := w.Cache.Advance(ctx, m, live, next, now, caughtUp)
		if err != nil {
			return err
		}
		if result == rediscache.LiveRetry {
			return nil
		} // CAS loss to another publisher; retry next poll
		if result != rediscache.LiveApplied {
			return fmt.Errorf("livebook: unexpected advance result %d", result)
		}
		raw, err := protojson.Marshal(next)
		if err != nil {
			return err
		}
		live = &rediscache.LiveBook{Snapshot: next, Sequence: next.GetMarketSequence(), Raw: string(raw)}
	}
	return nil
}

// Run isolates markets in independent poll loops. A slow or corrupt market
// must not serialize or block another market's L2 publication. Each request
// has a bounded deadline, and a failure never affects Kafka acknowledgement.
func (w *Worker) Run(ctx context.Context, every time.Duration, logger *slog.Logger) error {
	if w == nil || w.Journal == nil || w.Cache == nil || len(w.Markets) == 0 || logger == nil || every < 50*time.Millisecond {
		return errors.New("livebook: invalid worker configuration")
	}
	var wg sync.WaitGroup
	for _, market := range w.Markets {
		wg.Add(1)
		go func(m marketregistry.Market) {
			defer wg.Done()
			var lastError string
			var lastWarn time.Time
			tick := func() {
				started := time.Now()
				marketCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := w.Step(marketCtx, m, time.Now().UTC())
				cancel()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					if err.Error() != lastError || time.Since(lastWarn) > 10*time.Second {
						logger.Warn("Redis live book projection failed; retrying", "market", m.Symbol, "error", err)
						lastWarn = time.Now()
					}
					lastError = err.Error()
				} else {
					if lastError != "" {
						logger.Info("Redis live book projection recovered", "market", m.Symbol)
						lastError = ""
					}
					logger.Debug("Redis live book projection step completed", "market", m.Symbol, "duration", time.Since(started))
				}
			}
			tick()
			ticker := time.NewTicker(every)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					tick()
				}
			}
		}(market)
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}
