// Package recovery coordinates the deterministic book with durable Kafka
// partition progress. It does not depend on a Kafka or PostgreSQL driver.
package recovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"

	marketdatav1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matchingv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/book"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/trade"
	"google.golang.org/protobuf/proto"
)

var (
	ErrUnassigned          = errors.New("recovery: partition not assigned")
	ErrInvalidRecord       = errors.New("recovery: invalid Kafka record")
	ErrOffsetDiscontinuity = errors.New("recovery: offset discontinuity")
	ErrPoisoned            = errors.New("recovery: worker must restart from durable checkpoint")
	ErrUnknownMarket       = errors.New("recovery: market absent from immutable registry")
)

type Partition struct {
	Topic string
	ID    int32
}

type Record struct {
	Partition Partition
	Offset    int64
	Key       []byte
	Value     []byte
}

// MarketConfig binds one numeric matching market to one immutable Kafka
// partition and projection definition. The source publisher must use the same
// partition assignment and canonical decimal-string key.
type MarketConfig struct {
	PartitionID int32
	Definition  *marketdatav1.MarketProjectionDefinition
	Depth       uint32
}

type Assignment struct {
	NextOffset  int64
	FenceToken  string
	Checkpoints map[uint32][]byte
}

// Store atomically owns/loads a partition and, for each new Kafka record,
// persists one market checkpoint and advances its partition cursor. Save must
// fence stale owners and compare NextOffset with expectedOffset in a DB TX.
// DurableProjection is the one-and-only persistence payload for a Kafka record.
// The checkpoint, historical public trades, current L2 snapshot and cursor
// MUST commit in the same PostgreSQL transaction. Delta == nil only for an
// exact duplicate of the latest matcher result at a new Kafka offset.
type DurableProjection struct {
	MarketID   uint32
	Checkpoint []byte
	Envelope   []byte
	Delta      *marketdatav1.BookDelta
	Snapshot   *marketdatav1.BookSnapshot
	Trades     []*marketdatav1.TradeTick
}

type Store interface {
	Acquire(context.Context, Partition) (Assignment, error)
	Save(context.Context, Partition, string, int64, DurableProjection) error
	Release(context.Context, Partition, string) error
}

type session struct {
	assignment Assignment
	projectors map[uint32]*book.Projector
	poisoned   bool
}

type Worker struct {
	mu       sync.Mutex // one processing lane; rebalance callbacks safe
	topic    string
	store    Store
	markets  map[uint32]MarketConfig
	sessions map[Partition]*session
}

func New(topic string, store Store, markets []MarketConfig) (*Worker, error) {
	if topic == "" || store == nil || len(markets) == 0 {
		return nil, errors.New("recovery: topic, store and markets are required")
	}
	w := &Worker{topic: topic, store: store, markets: make(map[uint32]MarketConfig), sessions: make(map[Partition]*session)}
	for _, m := range markets {
		if m.PartitionID < 0 || m.Definition == nil || m.Depth == 0 {
			return nil, fmt.Errorf("recovery: invalid market config")
		}
		id := m.Definition.GetSourceMarketId()
		if _, exists := w.markets[id]; exists {
			return nil, fmt.Errorf("recovery: duplicate source_market_id %d", id)
		}
		if _, err := book.New(m.Definition, m.Depth); err != nil {
			return nil, err
		}
		w.markets[id] = m
	}
	return w, nil
}

// Assign restores every known market on the assigned partition, not merely
// the first one appearing in the next Kafka batch. Acquisition bumps a durable
// fence token, invalidating a prior worker instance on rebalances.
func (w *Worker) Assign(ctx context.Context, part Partition) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if part.Topic != w.topic || part.ID < 0 {
		return fmt.Errorf("%w: unexpected assignment %+v", ErrInvalidRecord, part)
	}
	if _, exists := w.sessions[part]; exists {
		return fmt.Errorf("recovery: duplicate partition assignment %+v", part)
	}
	state, err := w.store.Acquire(ctx, part)
	if err != nil {
		return fmt.Errorf("acquire partition %+v: %w", part, err)
	}
	if state.FenceToken == "" || state.NextOffset < 0 {
		_ = w.store.Release(ctx, part, state.FenceToken)
		return fmt.Errorf("recovery: invalid durable assignment %+v", part)
	}
	projectors := make(map[uint32]*book.Projector)
	for id, cfg := range w.markets {
		if cfg.PartitionID != part.ID {
			continue
		}
		data, has := state.Checkpoints[id]
		var p *book.Projector
		if has {
			p, err = book.Restore(cfg.Definition, cfg.Depth, data)
		} else {
			p, err = book.New(cfg.Definition, cfg.Depth)
		}
		if err != nil {
			_ = w.store.Release(ctx, part, state.FenceToken)
			return fmt.Errorf("restore market=%d partition=%d: %w", id, part.ID, err)
		}
		projectors[id] = p
	}
	for id := range state.Checkpoints {
		if _, exists := projectors[id]; !exists {
			_ = w.store.Release(ctx, part, state.FenceToken)
			return fmt.Errorf("%w: stored market=%d no longer configured for partition=%d", ErrUnknownMarket, id, part.ID)
		}
	}
	w.sessions[part] = &session{assignment: state, projectors: projectors}
	return nil
}

// Process returns a newly durable BookDelta, or nil for records already
// included in PostgreSQL. The caller may commit the Kafka offset ONLY on nil
// error. A Save failure poisons the session; the PROCESS MUST EXIT, drop its
// in-memory projector and restore from PG on the next assignment/restart.
// No Redis publishing happens here (CEX23.5 is the external publication stage).
func (w *Worker) Process(ctx context.Context, r Record) (*marketdatav1.BookDelta, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s, ok := w.sessions[r.Partition]
	if !ok {
		return nil, fmt.Errorf("%w: %+v", ErrUnassigned, r.Partition)
	}
	if s.poisoned {
		return nil, ErrPoisoned
	}
	if r.Partition.Topic != w.topic || r.Offset < 0 || r.Offset == math.MaxInt64 {
		return nil, fmt.Errorf("%w: topic or offset", ErrInvalidRecord)
	}
	if r.Offset < s.assignment.NextOffset {
		// Offset was atomically checkpointed by a previous owner. Never send
		// this record to the current projector (which rejects old sequences).
		return nil, nil
	}
	if r.Offset != s.assignment.NextOffset {
		return nil, fmt.Errorf("%w: expected Kafka offset=%d got=%d", ErrOffsetDiscontinuity, s.assignment.NextOffset, r.Offset)
	}
	if len(r.Value) == 0 {
		return nil, fmt.Errorf("%w: empty value", ErrInvalidRecord)
	}
	var envelope matchingv1.CommittedMatchingResult
	if err := proto.Unmarshal(r.Value, &envelope); err != nil {
		return nil, fmt.Errorf("%w: decode protobuf: %v", ErrInvalidRecord, err)
	}
	id := envelope.GetResult().GetCommand().GetMarketId()
	cfg, ok := w.markets[id]
	if !ok || cfg.PartitionID != r.Partition.ID {
		return nil, fmt.Errorf("%w: market=%d partition=%d", ErrUnknownMarket, id, r.Partition.ID)
	}
	if !bytes.Equal(r.Key, []byte(strconv.FormatUint(uint64(id), 10))) {
		return nil, fmt.Errorf("%w: noncanonical key for market=%d", ErrInvalidRecord, id)
	}
	p := s.projectors[id]
	delta, err := p.Apply(&envelope)
	if err != nil {
		return nil, fmt.Errorf("project market=%d offset=%d: %w", id, r.Offset, err)
	}
	// The publisher is at-least-once: an identical latest matching result may
	// appear at a *new* Kafka offset. Advance the durable partition cursor
	// without publishing a second delta. Older non-adjacent duplicates still
	// fail closed until a durable provenance index is implemented.
	checkpoint, err := p.MarshalCheckpoint()
	if err != nil {
		s.poisoned = true
		return nil, fmt.Errorf("%w: encode checkpoint: %v", ErrPoisoned, err)
	}
	// Extract public executions AFTER the book projector validated the full
	// matching result. An arithmetic/scale error poisons this session just
	// like a storage error: the in-memory state is ahead of PostgreSQL.
	var publicTrades []*marketdatav1.TradeTick
	if delta != nil {
		publicTrades, err = trade.Project(&envelope, cfg.Definition)
		if err != nil {
			s.poisoned = true
			return nil, fmt.Errorf("%w: project public trades: %v", ErrPoisoned, err)
		}
	}
	projection := DurableProjection{
		MarketID: id, Checkpoint: checkpoint, Envelope: append([]byte(nil), r.Value...),
		Delta: delta, Snapshot: p.Snapshot(), Trades: publicTrades,
	}
	if err := w.store.Save(ctx, r.Partition, s.assignment.FenceToken, r.Offset, projection); err != nil {
		s.poisoned = true
		return nil, fmt.Errorf("%w: persist partition=%d offset=%d: %v", ErrPoisoned, r.Partition.ID, r.Offset, err)
	}
	s.assignment.NextOffset = r.Offset + 1
	return delta, nil
}

func (w *Worker) Revoke(ctx context.Context, part Partition) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	s, ok := w.sessions[part]
	if !ok {
		return nil
	}
	delete(w.sessions, part)
	return w.store.Release(ctx, part, s.assignment.FenceToken)
}
