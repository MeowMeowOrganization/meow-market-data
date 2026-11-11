package readmodel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type PublishResult int

const (
	PublishApplied PublishResult = iota + 1
	PublishStale
)

var ErrStalePublication = errors.New("refresh: Redis cache rejected older projection")

type Publisher interface {
	Publish(context.Context, *Bundle) (PublishResult, error)
}

func refreshOne(ctx context.Context, reader *Reader, publisher Publisher, id uint32, now time.Time) error {
	b, err := reader.Load(ctx, id, now)
	if errors.Is(err, ErrNotReady) {
		return nil
	}
	if err != nil {
		return err
	}
	result, err := publisher.Publish(ctx, b)
	if err != nil {
		return err
	}
	switch result {
	case PublishApplied:
		return nil
	case PublishStale:
		return ErrStalePublication
	default:
		return fmt.Errorf("refresh: unexpected publish result %d", result)
	}
}

// RefreshAll remains usable by tests and administrative one-shot repair.
// Production uses independent per-market loops instead of one 30-second
// timeout shared by all markets.
func RefreshAll(ctx context.Context, reader *Reader, publisher Publisher, ids []uint32, now time.Time) error {
	if reader == nil || publisher == nil || len(ids) == 0 {
		return errors.New("refresh: missing reader, publisher or markets")
	}
	var failures []error
	for _, id := range ids {
		if err := refreshOne(ctx, reader, publisher, id, now); err != nil {
			failures = append(failures, fmt.Errorf("market %d: %w", id, err))
		}
	}
	return errors.Join(failures...)
}

// Run isolates each market's SQL/cache workload, including a per-market
// deadline. Redis failure NEVER propagates to Kafka acknowledgement.
// Independent markets continue refreshing even when one market is slow.
func Run(ctx context.Context, reader *Reader, publisher Publisher, ids []uint32, every time.Duration, logger *slog.Logger) error {
	if reader == nil || publisher == nil || len(ids) == 0 || every < time.Second || logger == nil {
		return errors.New("refresh: invalid worker configuration")
	}
	var wg sync.WaitGroup
	for _, marketID := range ids {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			var lastError string
			var lastWarn time.Time
			update := func() {
				started := time.Now()
				requestCtx, done := context.WithTimeout(ctx, 10*time.Second)
				err := refreshOne(requestCtx, reader, publisher, id, time.Now().UTC())
				done()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					if err.Error() != lastError || time.Since(lastWarn) >= 10*time.Second {
						logger.Warn("market-data Redis refresh failed; retrying", "market_id", id, "error", err)
						lastWarn = time.Now()
					}
					lastError = err.Error()
				} else {
					if lastError != "" {
						logger.Info("market-data Redis refresh recovered", "market_id", id)
					}
					lastError = ""
					logger.Debug("market-data Redis refresh completed", "market_id", id, "duration", time.Since(started))
				}
			}
			update()
			ticker := time.NewTicker(every)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					update()
				}
			}
		}(marketID)
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}
