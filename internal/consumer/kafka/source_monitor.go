package kafka

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type sourcePartition struct {
	stable   int64
	observed time.Time
	assigned bool
}

// SourceMonitor records broker-provided read-committed LSO samples, not Redis
// timestamps or Kafka consumer group commits. GET /source-healthz compares
// them with the PostgreSQL next_offset (the durable processed offset).
// An old broker sample is UNKNOWN, never incorrectly reported healthy.
type SourceMonitor struct {
	mu           sync.RWMutex
	partitions   map[int32]sourcePartition
	Topic        string
	DB           *sql.DB
	MaxLag       int64
	MaxSampleAge time.Duration
}

func NewSourceMonitor(db *sql.DB, topic string) *SourceMonitor {
	return &SourceMonitor{DB: db, Topic: topic, MaxLag: 100, MaxSampleAge: 30 * time.Second, partitions: map[int32]sourcePartition{}}
}
func (m *SourceMonitor) assigned(id int32) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitions[id] = sourcePartition{assigned: true}
}
func (m *SourceMonitor) revoked(id int32) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.partitions, id)
}
func (m *SourceMonitor) sampled(id int32, stable int64, at time.Time) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.partitions[id]; ok && v.assigned && stable >= 0 {
		m.partitions[id] = sourcePartition{assigned: true, stable: stable, observed: at}
	}
}

type SourceLag struct {
	Partition             int32     `json:"partition"`
	KafkaLastStableOffset int64     `json:"kafka_last_stable_offset"`
	PostgresNextOffset    int64     `json:"postgres_next_offset"`
	Lag                   int64     `json:"lag"`
	ObservedAt            time.Time `json:"observed_at"`
	Healthy               bool      `json:"healthy"`
	Reason                string    `json:"reason,omitempty"`
}

// Report reads the persisted cursor on each request (not an in-memory commit
// estimate). Health checks are intended for monitoring, not consumer fencing.
func (m *SourceMonitor) Report(ctx context.Context, now time.Time) ([]SourceLag, bool, error) {
	if m == nil || m.DB == nil || m.Topic == "" {
		return nil, false, fmt.Errorf("source monitor not configured")
	}
	m.mu.RLock()
	snap := make(map[int32]sourcePartition, len(m.partitions))
	for k, v := range m.partitions {
		snap[k] = v
	}
	m.mu.RUnlock()
	if len(snap) == 0 {
		return nil, false, nil
	}
	out := make([]SourceLag, 0, len(snap))
	ok := true
	for id, s := range snap {
		v := SourceLag{Partition: id, KafkaLastStableOffset: s.stable, ObservedAt: s.observed}
		e := m.DB.QueryRowContext(ctx, `SELECT next_offset FROM market_data.partition_cursors WHERE topic=$1 AND partition_id=$2`, m.Topic, id).Scan(&v.PostgresNextOffset)
		if e != nil {
			v.Reason = "postgres cursor unavailable"
			ok = false
			out = append(out, v)
			continue
		}
		v = assessSource(v, now, m.MaxLag, m.MaxSampleAge)
		if !v.Healthy {
			ok = false
		}
		out = append(out, v)
	}
	return out, ok, nil
}
func (m *SourceMonitor) Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	partitions, healthy, err := m.Report(ctx, time.Now().UTC())
	w.Header().Set("Content-Type", "application/json")
	if err != nil || !healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	_ = json.NewEncoder(w).Encode(struct {
		Healthy    bool        `json:"healthy"`
		Error      string      `json:"error,omitempty"`
		Partitions []SourceLag `json:"partitions"`
	}{healthy, errorText, partitions})
}

// assessSource is a pure policy function; offsets are Kafka read-committed
// last-stable-offset and PostgreSQL next_offset, NOT consumer-group commits.
func assessSource(v SourceLag, now time.Time, maxLag int64, maxAge time.Duration) SourceLag {
	if v.ObservedAt.IsZero() || now.Sub(v.ObservedAt) > maxAge || v.ObservedAt.After(now.Add(5*time.Second)) {
		v.Reason = "broker LSO sample unavailable or stale"
		return v
	}
	v.Lag = v.KafkaLastStableOffset - v.PostgresNextOffset
	if v.Lag < 0 {
		v.Reason = "durable cursor beyond sampled LSO"
	} else if v.Lag > maxLag {
		v.Reason = "source-to-projection offset lag exceeds limit"
	} else {
		v.Healthy = true
	}
	return v
}
