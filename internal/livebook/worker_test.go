package livebook

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	rc "github.com/MeowMeowOrganization/meow-market-data/internal/storage/redis"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var testMarket = marketregistry.Market{SourceID: 7, Symbol: "BTC-USDT", DefinitionVersion: 3, Depth: 2, PriceTickUnits: 25, QuantityStepAtoms: 1000}

func testSnapshot(seq uint64) *md.BookSnapshot {
	return &md.BookSnapshot{SourceMarketId: 7, MarketId: "BTC-USDT", MarketSequence: seq, MarketDefinitionVersion: 3, Depth: 2,
		Bids: []*md.BookLevel{{PriceUnits: 2500, BaseQuantityAtoms: "1000"}}}
}
func testDelta(seq uint64) *md.BookDelta {
	return &md.BookDelta{SourceMarketId: 7, MarketId: "BTC-USDT", MarketSequence: seq, PreviousMarketSequence: seq - 1, MarketDefinitionVersion: 3, Depth: 2}
}

type memoryJournal struct {
	snapshot      *md.BookSnapshot
	head          uint64
	deltas        []*md.BookDelta
	errorOnFetch  error
	snapshotReads int
}

func (j *memoryJournal) Snapshot(_ context.Context, _ marketregistry.Market) (*md.BookSnapshot, error) {
	j.snapshotReads++
	if j.snapshot == nil {
		return nil, readmodel.ErrNotReady
	}
	return proto.Clone(j.snapshot).(*md.BookSnapshot), nil
}
func (j *memoryJournal) Fetch(_ context.Context, _ marketregistry.Market, after uint64, limit int) (uint64, []*md.BookDelta, error) {
	if j.errorOnFetch != nil {
		return j.head, nil, j.errorOnFetch
	}
	if j.head < after {
		return j.head, nil, ErrSourceRegression
	}
	var out []*md.BookDelta
	current := after
	for _, d := range j.deltas {
		if d.GetMarketSequence() <= after {
			continue
		}
		if d.GetMarketSequence() != current+1 {
			return j.head, nil, ErrJournalGap
		}
		current = d.GetMarketSequence()
		out = append(out, d)
		if len(out) == limit {
			break
		}
	}
	if len(out) == 0 && j.head > after {
		return j.head, nil, ErrJournalGap
	}
	return j.head, out, nil
}

type memoryCache struct {
	live                                                              *rc.LiveBook
	seedCalls, advanceCalls, touchCalls, repairCalls, quarantineCalls int
	race                                                              bool
	corrupt                                                           *rc.CorruptLiveBookError
}

func record(s *md.BookSnapshot, now time.Time) *rc.LiveBook {
	raw, _ := protojson.Marshal(s)
	asof, _ := rc.AsOfSort(now)
	return &rc.LiveBook{Snapshot: proto.Clone(s).(*md.BookSnapshot), Sequence: s.GetMarketSequence(), Raw: string(raw), AsOfSort: asof}
}
func (c *memoryCache) Load(_ context.Context, _ marketregistry.Market, _ time.Time) (*rc.LiveBook, error) {
	if c.corrupt != nil {
		return nil, c.corrupt
	}
	if c.live == nil {
		return nil, rc.ErrUnavailableView
	}
	return c.live, nil
}
func (c *memoryCache) Seed(_ context.Context, _ marketregistry.Market, s *md.BookSnapshot, now time.Time) (rc.LiveResult, error) {
	c.seedCalls++
	if c.live != nil && c.live.Sequence > s.GetMarketSequence() {
		return rc.LiveRetry, nil
	}
	if c.live != nil && c.live.Sequence == s.GetMarketSequence() && !proto.Equal(c.live.Snapshot, s) {
		return rc.LiveConflict, rc.ErrLiveConflict
	}
	c.live = record(s, now)
	return rc.LiveApplied, nil
}
func (c *memoryCache) Advance(_ context.Context, _ marketregistry.Market, prev *rc.LiveBook, s *md.BookSnapshot, now time.Time, caughtUp bool) (rc.LiveResult, error) {
	c.advanceCalls++
	if c.race {
		c.race = false
		return rc.LiveRetry, nil
	}
	if c.live == nil || c.live.Sequence != prev.Sequence || c.live.Raw != prev.Raw {
		return rc.LiveRetry, nil
	}
	c.live = record(s, now)
	if !caughtUp {
		c.live.AsOfSort = "000000000000000000000000000000"
	}
	return rc.LiveApplied, nil
}
func (c *memoryCache) Touch(_ context.Context, _ marketregistry.Market, live *rc.LiveBook, now time.Time) (rc.LiveResult, error) {
	c.touchCalls++
	if c.live == nil || c.live.Sequence != live.Sequence {
		return rc.LiveRetry, nil
	}
	c.live = record(c.live.Snapshot, now)
	return rc.LiveApplied, nil
}
func (c *memoryCache) Repair(_ context.Context, _ marketregistry.Market, corrupt *rc.CorruptLiveBookError, s *md.BookSnapshot, now time.Time) (rc.LiveResult, error) {
	c.repairCalls++
	if c.corrupt == nil || c.corrupt.Raw != corrupt.Raw || c.corrupt.Sequence != s.GetMarketSequence() {
		return rc.LiveRetry, nil
	}
	c.corrupt = nil
	c.live = record(s, now)
	return rc.LiveApplied, nil
}
func (c *memoryCache) Quarantine(_ context.Context, _ marketregistry.Market, live *rc.LiveBook) (rc.LiveResult, error) {
	c.quarantineCalls++
	if c.live == nil || c.live.Sequence != live.Sequence || c.live.Raw != live.Raw {
		return rc.LiveRetry, nil
	}
	c.live.AsOfSort = "000000000000000000000000000000"
	return rc.LiveApplied, nil
}
func runStep(t *testing.T, w *Worker) {
	t.Helper()
	if err := w.Step(context.Background(), testMarket, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}
func TestColdBootstrapCatchupNoopAndIdleHeartbeat(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(1), head: 1, deltas: []*md.BookDelta{testDelta(1)}}
	c := &memoryCache{}
	w := &Worker{Journal: j, Cache: c}
	runStep(t, w)
	if c.live.Sequence != 1 || c.seedCalls != 1 {
		t.Fatalf("bootstrap: %+v", c.live)
	}
	j.head = 3
	j.snapshot = testSnapshot(3)
	j.deltas = append(j.deltas, testDelta(2), testDelta(3))
	runStep(t, w)
	if c.live.Sequence != 3 || c.advanceCalls != 2 || c.seedCalls != 1 {
		t.Fatalf("delta replay: %+v", c)
	}
	c.live.AsOfSort = "000000000000000000000000000000"
	runStep(t, w)
	if c.touchCalls != 1 || c.live.Sequence != 3 {
		t.Fatalf("idle touch: %+v", c)
	}
	runStep(t, w)
	if c.touchCalls != 1 {
		t.Fatal("idle source caused unnecessary touch")
	}
}
func TestGapTriggersFullSnapshotRecovery(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(5), head: 5, deltas: []*md.BookDelta{testDelta(4), testDelta(5)}}
	c := &memoryCache{live: record(testSnapshot(2), time.Now().UTC())}
	w := &Worker{Journal: j, Cache: c}
	runStep(t, w)
	if c.live.Sequence != 5 || c.seedCalls != 1 || c.advanceCalls != 0 {
		t.Fatalf("gap was applied as deltas: %+v", c)
	}
}
func TestRedisEvictionAndSourceRegression(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(5), head: 5}
	c := &memoryCache{live: record(testSnapshot(6), time.Now().UTC())}
	w := &Worker{Journal: j, Cache: c}
	err := w.Step(context.Background(), testMarket, time.Now().UTC())
	if !errors.Is(err, ErrSourceRegression) || c.live.Sequence != 6 {
		t.Fatalf("source reset must fail closed: %v", err)
	}
	c.live = nil
	runStep(t, w)
	if c.live.Sequence != 5 {
		t.Fatal("Redis eviction not repaired")
	}
}
func TestCASRaceRetriesAndCorruptRepair(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(2), head: 2, deltas: []*md.BookDelta{testDelta(2)}}
	c := &memoryCache{live: record(testSnapshot(1), time.Now().UTC()), race: true}
	w := &Worker{Journal: j, Cache: c}
	runStep(t, w)
	if c.live.Sequence != 1 {
		t.Fatal("CAS retry must not incorrectly advance")
	}
	runStep(t, w)
	if c.live.Sequence != 2 {
		t.Fatal("CAS retry did not recover")
	}
	c.corrupt = &rc.CorruptLiveBookError{Sequence: 2, Raw: "invalid-protobuf-json"}
	runStep(t, w)
	if c.repairCalls != 1 || c.corrupt != nil || c.live.Sequence != 2 {
		t.Fatalf("corrupt repair: %+v", c)
	}
}
func TestCorruptRepairCannotRewindAndInvalidDeltaFails(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(4), head: 4}
	c := &memoryCache{corrupt: &rc.CorruptLiveBookError{Sequence: 5, Raw: "corrupt"}}
	w := &Worker{Journal: j, Cache: c}
	err := w.Step(context.Background(), testMarket, time.Now().UTC())
	if !errors.Is(err, ErrSourceRegression) || c.repairCalls != 0 {
		t.Fatalf("rewind was accepted: %v", err)
	}
	c.corrupt = nil
	c.live = record(testSnapshot(3), time.Now().UTC())
	j.deltas = []*md.BookDelta{testDelta(4)}
	j.deltas[0].BidUpdates = []*md.BookLevelUpdate{{PriceUnits: 2501, BaseQuantityAtoms: "1000"}}
	err = w.Step(context.Background(), testMarket, time.Now().UTC())
	if err == nil || c.live.Sequence != 3 || c.seedCalls != 0 {
		t.Fatalf("corrupt delta must fail rather than resnapshot: %v", err)
	}
}
func TestJournalErrorsDoNotMutateRedis(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(2), head: 2, errorOnFetch: fmt.Errorf("PG connection failed")}
	c := &memoryCache{live: record(testSnapshot(1), time.Now().UTC())}
	err := (&Worker{Journal: j, Cache: c}).Step(context.Background(), testMarket, time.Now().UTC())
	if err == nil || c.live.Sequence != 1 || c.seedCalls != 0 {
		t.Fatalf("unexpected mutation after DB failure: %v", err)
	}
}

func TestSameSequenceParityAuditFailsClosed(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(10), head: 10}
	wrong := testSnapshot(10)
	wrong.Bids[0].BaseQuantityAtoms = "2000" // valid but not authoritative
	c := &memoryCache{live: record(wrong, time.Now().UTC())}
	c.live.AsOfSort = "000000000000000000000000000000"
	err := (&Worker{Journal: j, Cache: c}).Step(context.Background(), testMarket, time.Now().UTC())
	if !errors.Is(err, rc.ErrLiveConflict) || c.touchCalls != 0 || c.live.Sequence != 10 || c.quarantineCalls != 1 || c.live.AsOfSort != "000000000000000000000000000000" {
		t.Fatalf("divergent cache was incorrectly certified fresh: %v", err)
	}
}

func TestBacklogIsNotCertifiedFreshUntilDurableHead(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(131), head: 131}
	for seq := uint64(2); seq <= 131; seq++ {
		j.deltas = append(j.deltas, testDelta(seq))
	}
	c := &memoryCache{live: record(testSnapshot(1), time.Now().UTC())}
	w := &Worker{Journal: j, Cache: c}
	runStep(t, w)
	if c.live.Sequence != 129 || c.live.AsOfSort != "000000000000000000000000000000" {
		t.Fatalf("partial catch-up appeared fresh: seq=%d asof=%s", c.live.Sequence, c.live.AsOfSort)
	}
	runStep(t, w)
	if c.live.Sequence != 131 || c.live.AsOfSort == "000000000000000000000000000000" {
		t.Fatalf("catch-up never certified at head: seq=%d asof=%s", c.live.Sequence, c.live.AsOfSort)
	}
}

func TestFutureDatedFreshnessIsRechecked(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(9), head: 9}
	c := &memoryCache{live: record(testSnapshot(9), time.Now().UTC())}
	c.live.AsOfSort = "999999999999999999999999999999"
	w := &Worker{Journal: j, Cache: c}
	runStep(t, w)
	if c.touchCalls != 1 || c.live.AsOfSort == "999999999999999999999999999999" {
		t.Fatal("future asof was not repaired")
	}
}

func TestDivergentPredecessorCannotBeCertifiedAtHead(t *testing.T) {
	j := &memoryJournal{snapshot: testSnapshot(2), head: 2, deltas: []*md.BookDelta{testDelta(2)}}
	wrong := testSnapshot(1)
	wrong.Bids[0].BaseQuantityAtoms = "2000"
	c := &memoryCache{live: record(wrong, time.Now().UTC())}
	err := (&Worker{Journal: j, Cache: c}).Step(context.Background(), testMarket, time.Now().UTC())
	if !errors.Is(err, rc.ErrLiveConflict) || c.live.Sequence != 1 || c.advanceCalls != 0 || c.quarantineCalls != 1 {
		t.Fatalf("divergent book was certified or advanced: %v", err)
	}
}
