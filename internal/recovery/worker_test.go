package recovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	marketv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	marketdatav1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matchingv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/book"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var part = Partition{Topic: "matching.results.committed.v1", ID: 0}

type state struct {
	next       int64
	token      string
	snapshots  map[uint32][]byte
	provenance map[uint32]map[uint64][32]byte
}
type memoryStore struct {
	mu         sync.Mutex
	states     map[Partition]*state
	generation int
	failSave   bool
	writes     int
}

func newStore() *memoryStore { return &memoryStore{states: make(map[Partition]*state)} }
func (m *memoryStore) Acquire(_ context.Context, p Partition) (Assignment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.states[p]
	if s == nil {
		s = &state{snapshots: make(map[uint32][]byte), provenance: make(map[uint32]map[uint64][32]byte)}
		m.states[p] = s
	}
	m.generation++
	s.token = fmt.Sprintf("owner-%d", m.generation)
	copySnapshots := make(map[uint32][]byte)
	for k, v := range s.snapshots {
		copySnapshots[k] = append([]byte(nil), v...)
	}
	return Assignment{NextOffset: s.next, FenceToken: s.token, Checkpoints: copySnapshots}, nil
}
func (m *memoryStore) Save(_ context.Context, p Partition, tok string, offset int64, projection DurableProjection) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failSave {
		return errors.New("postgres unavailable")
	}
	s := m.states[p]
	if s == nil || s.token != tok {
		return errors.New("stale fence")
	}
	if s.next != offset {
		return errors.New("cursor conflict")
	}
	if projection.HistoricalDuplicate {
		if projection.Delta != nil || len(projection.Trades) != 0 {
			return errors.New("duplicate mutated data")
		}
		var env matchingv1.CommittedMatchingResult
		if e := proto.Unmarshal(projection.Envelope, &env); e != nil {
			return e
		}
		canonical, e := (proto.MarshalOptions{Deterministic: true}).Marshal(&env)
		if e != nil {
			return e
		}
		digest := sha256.Sum256(canonical)
		if digest != s.provenance[projection.MarketID][env.GetResult().GetMarketSequence()] {
			return errors.New("duplicate digest mismatch")
		}
		if string(projection.Checkpoint) != string(s.snapshots[projection.MarketID]) {
			return errors.New("duplicate modified checkpoint")
		}
	} else {
		s.snapshots[projection.MarketID] = append([]byte(nil), projection.Checkpoint...)
		if projection.Delta != nil {
			var env matchingv1.CommittedMatchingResult
			if e := proto.Unmarshal(projection.Envelope, &env); e != nil {
				return e
			}
			canonical, e := (proto.MarshalOptions{Deterministic: true}).Marshal(&env)
			if e != nil {
				return e
			}
			if s.provenance[projection.MarketID] == nil {
				s.provenance[projection.MarketID] = map[uint64][32]byte{}
			}
			s.provenance[projection.MarketID][env.GetResult().GetMarketSequence()] = sha256.Sum256(canonical)
		}
	}
	s.next++
	m.writes++
	return nil
}
func (m *memoryStore) ProvenanceDigest(_ context.Context, p Partition, id uint32, seq uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.states[p]; s != nil {
		if digest, ok := s.provenance[id][seq]; ok {
			return append([]byte(nil), digest[:]...), nil
		}
	}
	return nil, errors.New("missing immutable provenance")
}
func (m *memoryStore) Release(_ context.Context, p Partition, tok string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.states[p]
	if s != nil && s.token == tok {
		s.token = ""
	}
	return nil
}
func definition(id uint32, symbol string) *marketdatav1.MarketProjectionDefinition {
	return &marketdatav1.MarketProjectionDefinition{SourceMarketId: id, DefinitionVersion: 3, Market: &marketv1.Market{
		MarketId: symbol, PriceTickUnits: 25, QuantityStepBaseAtoms: 1000,
	}}
}
func makeWorker(t *testing.T, store Store, markets ...MarketConfig) *Worker {
	t.Helper()
	if len(markets) == 0 {
		markets = []MarketConfig{{PartitionID: 0, Depth: 2, Definition: definition(7, "BTC-USDT")}}
	}
	w, err := New(part.Topic, store, markets)
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func record(offset int64, market uint32, seq uint64, id uint64) *Record {
	result := &matchingv1.CommittedMatchingResult{
		Result: &matchingv1.MatchingResult{
			MarketSequence: seq,
			Command: &matchingv1.MatchingCommand{
				MarketId: market, CommandId: fmt.Sprintf("place-%d", id),
				Command: &matchingv1.MatchingCommand_PlaceLimitOrder{PlaceLimitOrder: &matchingv1.PlaceLimitOrder{
					OrderId: id, Side: orderv1.OrderSide_ORDER_SIDE_BUY, PriceTicks: int64(100 + id), QuantityLots: 5,
				}},
			},
		},
		MarketDefinitionVersion: 3,
		RecordedAt:              timestamppb.New(time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)),
	}
	data, err := proto.Marshal(result)
	if err != nil {
		panic(err)
	}
	return &Record{Partition: part, Offset: offset, Key: fmt.Append(nil, market), Value: data}
}
func process(t *testing.T, w *Worker, r *Record) {
	t.Helper()
	d, err := w.Process(context.Background(), *r)
	if err != nil || d == nil {
		t.Fatalf("process: delta=%v err=%v", d, err)
	}
}
func assign(t *testing.T, w *Worker) {
	t.Helper()
	if err := w.Assign(context.Background(), part); err != nil {
		t.Fatal(err)
	}
}

func TestCrashRecoveryAndDuplicateKafkaOffsets(t *testing.T) {
	store := newStore()
	w := makeWorker(t, store)
	assign(t, w)
	r0 := record(0, 7, 1, 1)
	r1 := record(1, 7, 2, 2)
	process(t, w, r0)
	process(t, w, r1)
	if err := w.Revoke(context.Background(), part); err != nil {
		t.Fatal(err)
	}
	w2 := makeWorker(t, store)
	assign(t, w2)
	d, err := w2.Process(context.Background(), *r0)
	if err != nil || d != nil {
		t.Fatalf("old redelivery %v %v", d, err)
	}
	d, err = w2.Process(context.Background(), *r1)
	if err != nil || d != nil {
		t.Fatalf("last redelivery %v %v", d, err)
	}
	process(t, w2, record(2, 7, 3, 3))
	if store.writes != 3 {
		t.Fatalf("expected 3 durable records, got %d", store.writes)
	}
}
func TestSaveFailurePoisonsAndRestores(t *testing.T) {
	store := newStore()
	w := makeWorker(t, store)
	assign(t, w)
	process(t, w, record(0, 7, 1, 1))
	store.failSave = true
	_, err := w.Process(context.Background(), *record(1, 7, 2, 2))
	if !errors.Is(err, ErrPoisoned) {
		t.Fatal(err)
	}
	store.failSave = false
	_, err = w.Process(context.Background(), *record(1, 7, 2, 2))
	if !errors.Is(err, ErrPoisoned) {
		t.Fatal("session resumed after unknown commit", err)
	}
	w2 := makeWorker(t, store)
	assign(t, w2) // steals durable fence
	process(t, w2, record(1, 7, 2, 2))
}
func TestOffsetGapAndInvalidKeyNoProgress(t *testing.T) {
	store := newStore()
	w := makeWorker(t, store)
	assign(t, w)
	_, err := w.Process(context.Background(), *record(1, 7, 1, 1))
	if !errors.Is(err, ErrOffsetDiscontinuity) {
		t.Fatal(err)
	}
	r := record(0, 7, 1, 1)
	r.Key = []byte("007")
	_, err = w.Process(context.Background(), *r)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatal(err)
	}
	process(t, w, record(0, 7, 1, 1))
	if store.writes != 1 {
		t.Fatal("invalid record committed")
	}
}
func TestMultipleMarketsOnePartitionRecovered(t *testing.T) {
	store := newStore()
	registry := []MarketConfig{{PartitionID: 0, Depth: 2, Definition: definition(7, "BTC-USDT")}, {PartitionID: 0, Depth: 2, Definition: definition(8, "ETH-USDT")}}
	w := makeWorker(t, store, registry...)
	assign(t, w)
	process(t, w, record(0, 7, 1, 1))
	process(t, w, record(1, 8, 1, 1))
	process(t, w, record(2, 7, 2, 2))
	w2 := makeWorker(t, store, registry...)
	assign(t, w2)
	process(t, w2, record(3, 8, 2, 2))
	if store.writes != 4 {
		t.Fatal("unexpected durable market cursor")
	}
}
func TestStaleFenceAndUnknownMarket(t *testing.T) {
	store := newStore()
	w := makeWorker(t, store)
	assign(t, w)
	w2 := makeWorker(t, store)
	assign(t, w2) // simulates reassignment
	_, err := w.Process(context.Background(), *record(0, 7, 1, 1))
	if !errors.Is(err, ErrPoisoned) {
		t.Fatal(err)
	}
	process(t, w2, record(0, 7, 1, 1))
	_, err = w2.Process(context.Background(), *record(1, 8, 1, 1))
	if !errors.Is(err, ErrUnknownMarket) {
		t.Fatal(err)
	}
}

func TestDuplicateMatchingResultAtNewKafkaOffset(t *testing.T) {
	store := newStore()
	w := makeWorker(t, store)
	assign(t, w)
	r0 := record(0, 7, 1, 10)
	process(t, w, r0)
	duplicate := record(1, 7, 1, 10)
	d, err := w.Process(context.Background(), *duplicate)
	if err != nil || d != nil {
		t.Fatalf("duplicate result produced delta=%v error=%v", d, err)
	}
	process(t, w, record(2, 7, 2, 11))
	if store.writes != 3 {
		t.Fatal("duplicate source offset was not checkpointed")
	}
}

func TestHistoricalDuplicatePreservesStateAndAdvancesCursor(t *testing.T) {
	store := newStore()
	w := makeWorker(t, store)
	assign(t, w)
	first := record(0, 7, 1, 10)
	process(t, w, first)
	process(t, w, record(1, 7, 2, 11))
	process(t, w, record(2, 7, 3, 12))
	before := append([]byte(nil), store.states[part].snapshots[7]...)
	older := *first
	older.Offset = 3
	delta, err := w.Process(context.Background(), older)
	if err != nil || delta != nil || store.states[part].next != 4 || string(before) != string(store.states[part].snapshots[7]) {
		t.Fatalf("historical replay delta=%v err=%v", delta, err)
	}
	// A different payload at the same source identity must stop processing.
	bad := record(4, 7, 1, 99)
	_, err = w.Process(context.Background(), *bad)
	if !errors.Is(err, book.ErrConflictingReplay) || store.states[part].next != 4 {
		t.Fatalf("accepted conflicting historical replay: %v", err)
	}
	// Restored workers retain history from the durable provenance table.
	if err = w.Revoke(context.Background(), part); err != nil {
		t.Fatal(err)
	}
	w2 := makeWorker(t, store)
	assign(t, w2)
	older.Offset = 4
	if d, e := w2.Process(context.Background(), older); e != nil || d != nil {
		t.Fatalf("replay after restore delta=%v err=%v", d, e)
	}
}
func TestMissingHistoricalProvenanceFailsClosed(t *testing.T) {
	store := newStore()
	w := makeWorker(t, store)
	assign(t, w)
	process(t, w, record(0, 7, 1, 10))
	process(t, w, record(1, 7, 2, 11))
	delete(store.states[part].provenance[7], 1)
	r := record(2, 7, 1, 10)
	if _, e := w.Process(context.Background(), *r); e == nil || store.states[part].next != 2 {
		t.Fatalf("missing provenance accepted: %v", e)
	}
}
