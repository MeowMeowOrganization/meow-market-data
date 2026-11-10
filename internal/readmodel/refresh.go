package readmodel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type Publisher interface {
	Publish(context.Context, *Bundle) error
}

// RefreshAll is independently retryable and never participates in Kafka
// acknowledgement. Its source is the coherent PostgreSQL public view, not
// the in-memory projector or Redis. Can be run after Redis FLUSHALL.
func RefreshAll(ctx context.Context, reader *Reader, publisher Publisher, ids []uint32, now time.Time) error {
	if reader == nil || publisher == nil || len(ids) == 0 {
		return errors.New("refresh: missing reader, publisher or markets")
	}
	var failures []error
	for _, id := range ids {
		b, err := reader.Load(ctx, id, now)
		if errors.Is(err, ErrNotReady) {
			continue
		} // no matching results for market yet
		if err == nil {
			err = publisher.Publish(ctx, b)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("market %d: %w", id, err))
		}
	}
	return errors.Join(failures...)
}

// Run refreshes rolling 24h windows even if the matching topic is idle.
// Failures are logged/retried; Redis unavailability MUST NOT block Kafka or
// financial settlement. The latest coherent PG view repairs the entire cache.
func Run(ctx context.Context, reader *Reader, publisher Publisher, ids []uint32, every time.Duration, logger *slog.Logger) error {
	if every <= 0 || logger == nil {
		return errors.New("refresh: positive interval and logger required")
	}
	update := func() {
		timeout, done := context.WithTimeout(ctx, 30*time.Second)
		defer done()
		if err := RefreshAll(timeout, reader, publisher, ids, time.Now().UTC()); err != nil && ctx.Err() == nil {
			logger.Warn("market-data Redis refresh failed; retrying", "error", err)
		}
	}
	update()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			update()
		}
	}
}
