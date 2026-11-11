package kafka

import (
	"testing"
	"time"
)

func TestSourceLagAndStaleness(t *testing.T) {
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	base := SourceLag{Partition: 1, KafkaLastStableOffset: 110, PostgresNextOffset: 100, ObservedAt: now.Add(-2 * time.Second)}
	if v := assessSource(base, now, 10, 30*time.Second); !v.Healthy || v.Lag != 10 {
		t.Fatalf("boundary lag: %+v", v)
	}
	if v := assessSource(base, now, 9, 30*time.Second); v.Healthy {
		t.Fatalf("excess lag accepted: %+v", v)
	}
	base.PostgresNextOffset = 111
	if v := assessSource(base, now, 10, 30*time.Second); v.Healthy {
		t.Fatalf("cursor ahead of LSO: %+v", v)
	}
	base.PostgresNextOffset = 100
	base.ObservedAt = now.Add(-time.Minute)
	if v := assessSource(base, now, 10, 30*time.Second); v.Healthy {
		t.Fatalf("stale sample accepted: %+v", v)
	}
}
