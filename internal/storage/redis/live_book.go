package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bookcheck"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

// Live L2 book keys are independent from the periodically refreshed cache
// bundle. A full refresh must never rewind an incremental book sequence.
// Both keys share one Redis Cluster hash slot and all operations are atomic.
const readLiveBook = `
local seq = redis.call('HGET', KEYS[1], 'sequence')
local padded = redis.call('HGET', KEYS[1], 'sequence_sort')
local asof = redis.call('HGET', KEYS[1], 'asof_sort')
local book = redis.call('GET', KEYS[2])
if not seq or not padded or not asof or not book or #seq < 1 or #seq > 20 or
   #padded ~= 20 or #asof ~= 30 or string.find(seq, '[^0-9]') or
   string.find(padded, '[^0-9]') or string.find(asof, '[^0-9]') or
   (#seq > 1 and string.sub(seq, 1, 1) == '0') or
   padded ~= string.rep('0', 20 - #seq) .. seq then return nil end
if ARGV[1] == '1' and (asof < ARGV[2] or asof > ARGV[3]) then return nil end
return {seq, asof, book}
`

// Snapshot seeding is monotonic and can jump forward after a gap because the
// source is a complete validated PostgreSQL snapshot, not a journal delta.
// A same-sequence different payload is a conflict rather than silent repair.
const seedLiveBook = `
local old = redis.call('HGET', KEYS[1], 'sequence_sort')
local payload = redis.call('GET', KEYS[2])
if old and #old == 20 and not string.find(old, '[^0-9]') then
  if old > ARGV[1] then return 0 end
  if old == ARGV[1] and payload and payload ~= ARGV[4] then return 2 end
end
redis.call('SET', KEYS[2], ARGV[4])
redis.call('HSET', KEYS[1], 'sequence_sort', ARGV[1], 'sequence', ARGV[3], 'asof_sort', ARGV[2])
return 1
`

// Compare-and-swap uses both the exact ASCII uint64 predecessor and the
// previously read JSON. Multiple projector replicas cannot lose an update.
// Redis is not the canonical replay cursor; PostgreSQL remains that source.
const advanceLiveBook = `
local previous = redis.call('HGET', KEYS[1], 'sequence_sort')
local oldbook = redis.call('GET', KEYS[2])
if not previous or previous ~= ARGV[1] or not oldbook or oldbook ~= ARGV[2] then return 0 end
redis.call('SET', KEYS[2], ARGV[6])
redis.call('HSET', KEYS[1], 'sequence_sort', ARGV[3], 'sequence', ARGV[4], 'asof_sort', ARGV[5])
return 1
`

const touchLiveBook = `
if redis.call('HGET', KEYS[1], 'sequence_sort') ~= ARGV[1] or
   redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
local oldasof = redis.call('HGET', KEYS[1], 'asof_sort')
if not oldasof or #oldasof ~= 30 or string.find(oldasof, '[^0-9]') or
   oldasof > ARGV[4] or oldasof < ARGV[3] then
  redis.call('HSET', KEYS[1], 'asof_sort', ARGV[3])
end
return 1
`

// Invalidate a valid-looking but divergent or uncertified live book. The
// compare-and-swap prevents a delayed audit from invalidating newer state.
const quarantineLiveBook = `
if redis.call('HGET', KEYS[1], 'sequence_sort') ~= ARGV[1] or
   redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], 'asof_sort', '000000000000000000000000000000')
return 1
`

// Repairs a *proven invalid* same-sequence payload only if its exact bytes
// have not changed since validation failed. It cannot overwrite another
// publisher or accept a different valid book with the same sequence.
const repairCorruptLiveBook = `
if redis.call('HGET', KEYS[1], 'sequence_sort') ~= ARGV[1] or
   redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
redis.call('SET', KEYS[2], ARGV[5])
redis.call('HSET', KEYS[1], 'sequence_sort', ARGV[1], 'sequence', ARGV[3], 'asof_sort', ARGV[4])
return 1
`

var ErrLiveConflict = errors.New("redis: conflicting same-sequence live book")

// CorruptLiveBookError carries only state that was read atomically. The worker
// can use it as a CAS token after independently verifying a durable snapshot.
// Do not treat a valid but different same-sequence book as corrupt.
type CorruptLiveBookError struct {
	Sequence uint64
	Raw      string
}

func (e *CorruptLiveBookError) Error() string { return "redis: invalid live book payload" }

type LiveResult int

const (
	LiveApplied LiveResult = iota + 1
	LiveRetry
	LiveConflict
)

type LiveBook struct {
	Snapshot *md.BookSnapshot
	Sequence uint64
	Raw      string
	AsOfSort string
}

func marketExpected(m marketregistry.Market) bookcheck.Expected {
	return bookcheck.Expected{SourceMarketID: m.SourceID, MarketID: m.Symbol, DefinitionVersion: m.DefinitionVersion,
		Depth: m.Depth, PriceTickUnits: m.PriceTickUnits, QuantityStepBaseAtoms: m.QuantityStepAtoms}
}
func liveKeys(ns Namespace, m marketregistry.Market) []string {
	return []string{ns.Key(m.SourceID, m.DefinitionVersion, "book:live:state"), ns.Key(m.SourceID, m.DefinitionVersion, "book:live")}
}
func liveSequence(n uint64) (string, string) {
	raw := strconv.FormatUint(n, 10)
	padded, _ := padDecimal(raw, 20)
	return padded, raw
}
func liveTime(t time.Time) (string, error) {
	if t.Before(time.Unix(0, 0)) || t.IsZero() || t.After(time.Now().UTC().Add(5*time.Second)) {
		return "", errors.New("redis: invalid live book freshness timestamp")
	}
	return padDecimal(postgres.NanosDecimal(t.UTC()), 30)
}

// LoadLiveBook returns a validated atomic book view. With requireFresh=false
// workers can recover a stale-but-complete view and safely catch it up from PG.
// Public readers require a successful source health check within 20 seconds.
func LoadLiveBook(ctx context.Context, client *goredis.Client, ns Namespace, m marketregistry.Market, now time.Time, requireFresh bool) (*LiveBook, error) {
	if client == nil || !ns.Valid() || m.Depth == 0 || m.DefinitionVersion == 0 {
		return nil, ErrUnavailableView
	}
	lower, e1 := padDecimal(postgres.NanosDecimal(now.UTC().Add(-20*time.Second)), 30)
	upper, e2 := padDecimal(postgres.NanosDecimal(now.UTC().Add(5*time.Second)), 30)
	if e1 != nil || e2 != nil {
		return nil, ErrUnavailableView
	}
	fresh := "0"
	if requireFresh {
		fresh = "1"
	}
	value, err := client.Eval(ctx, readLiveBook, liveKeys(ns, m), fresh, lower, upper).Result()
	if errors.Is(err, goredis.Nil) || value == nil {
		return nil, ErrUnavailableView
	}
	if err != nil {
		return nil, fmt.Errorf("redis: read live book: %w", err)
	}
	parts, ok := value.([]interface{})
	if !ok || len(parts) != 3 {
		return nil, ErrUnavailableView
	}
	seq, ok1 := parts[0].(string)
	asof, ok2 := parts[1].(string)
	raw, ok3 := parts[2].(string)
	if !ok1 || !ok2 || !ok3 {
		return nil, ErrUnavailableView
	}
	n, err := strconv.ParseUint(seq, 10, 64)
	if err != nil || n == 0 {
		return nil, ErrUnavailableView
	}
	snapshot := new(md.BookSnapshot)
	if protojson.Unmarshal([]byte(raw), snapshot) != nil || snapshot.GetMarketSequence() != n ||
		bookcheck.Validate(snapshot, marketExpected(m)) != nil {
		return nil, &CorruptLiveBookError{Sequence: n, Raw: raw}
	}
	return &LiveBook{Snapshot: snapshot, Sequence: n, Raw: raw, AsOfSort: asof}, nil
}

// SeedLiveBook is used exclusively with a coherent PostgreSQL full snapshot.
// Result is Applied, Retry (another writer is ahead), or Conflict.
func (c *Cache) SeedLiveBook(ctx context.Context, m marketregistry.Market, snapshot *md.BookSnapshot, now time.Time) (LiveResult, error) {
	if c == nil || c.Client == nil || !c.Namespace.Valid() {
		return 0, ErrUnavailableView
	}
	if err := bookcheck.Validate(snapshot, marketExpected(m)); err != nil {
		return 0, err
	}
	asof, err := liveTime(now)
	if err != nil {
		return 0, err
	}
	payload, err := marshaledMessage(snapshot)
	if err != nil {
		return 0, err
	}
	padded, raw := liveSequence(snapshot.GetMarketSequence())
	result, err := c.Client.Eval(ctx, seedLiveBook, liveKeys(c.Namespace, m), padded, asof, raw, payload).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: seed live book: %w", err)
	}
	switch result {
	case int64(0):
		return LiveRetry, nil
	case int64(1):
		return LiveApplied, nil
	case int64(2):
		return LiveConflict, ErrLiveConflict
	default:
		return 0, fmt.Errorf("redis: unexpected seed status %v", result)
	}
}

// AdvanceLiveBook is conditional on the exact state read by the worker.
// A race returns LiveRetry and must be resolved by re-reading Redis + PG.
func (c *Cache) AdvanceLiveBook(ctx context.Context, m marketregistry.Market, previous *LiveBook, next *md.BookSnapshot, now time.Time, caughtUp bool) (LiveResult, error) {
	if c == nil || c.Client == nil || !c.Namespace.Valid() || previous == nil || previous.Snapshot == nil ||
		next == nil || previous.Sequence == ^uint64(0) || next.GetMarketSequence() != previous.Sequence+1 ||
		bookcheck.Validate(next, marketExpected(m)) != nil {
		return 0, errors.New("redis: invalid live book advance")
	}
	asof, err := liveTime(now)
	if err != nil {
		return 0, err
	}
	// A published delta is not a verified live view if more committed deltas
	// remain unread. Public readers will fall back to durable PG meanwhile.
	if !caughtUp {
		asof = "000000000000000000000000000000"
	}
	payload, err := marshaledMessage(next)
	if err != nil {
		return 0, err
	}
	old, _ := liveSequence(previous.Sequence)
	padded, raw := liveSequence(next.GetMarketSequence())
	result, err := c.Client.Eval(ctx, advanceLiveBook, liveKeys(c.Namespace, m), old, previous.Raw, padded, raw, asof, payload).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: advance live book: %w", err)
	}
	switch result {
	case int64(0):
		return LiveRetry, nil
	case int64(1):
		return LiveApplied, nil
	}
	return 0, fmt.Errorf("redis: unexpected advance status %v", result)
}

// Touch marks a successfully checked-up source as fresh even if the market
// was idle. It does NOT advance sequence without a durable journal record.
func (c *Cache) TouchLiveBook(ctx context.Context, m marketregistry.Market, live *LiveBook, now time.Time) (LiveResult, error) {
	if c == nil || c.Client == nil || live == nil {
		return 0, ErrUnavailableView
	}
	asof, err := liveTime(now)
	if err != nil {
		return 0, err
	}
	seq, _ := liveSequence(live.Sequence)
	upper, err := AsOfSort(now.UTC().Add(5 * time.Second))
	if err != nil {
		return 0, err
	}
	result, err := c.Client.Eval(ctx, touchLiveBook, liveKeys(c.Namespace, m), seq, live.Raw, asof, upper).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: touch live book: %w", err)
	}
	if result == int64(0) {
		return LiveRetry, nil
	}
	if result == int64(1) {
		return LiveApplied, nil
	}
	return 0, fmt.Errorf("redis: unexpected touch status %v", result)
}

// AsOfSort returns a fixed-width timestamp comparable without float conversion.
func AsOfSort(t time.Time) (string, error) {
	if t.Before(time.Unix(0, 0)) {
		return "", errors.New("redis: invalid timestamp")
	}
	return padDecimal(postgres.NanosDecimal(t.UTC()), 30)
}

// RepairCorruptLiveBook is only for a snapshot validated by the PG journal
// with exactly the corrupted cache sequence. All bytes are CAS-fenced.
func (c *Cache) RepairCorruptLiveBook(ctx context.Context, m marketregistry.Market, corrupt *CorruptLiveBookError, durable *md.BookSnapshot, now time.Time) (LiveResult, error) {
	if c == nil || c.Client == nil || corrupt == nil || durable == nil || corrupt.Sequence != durable.GetMarketSequence() ||
		bookcheck.Validate(durable, marketExpected(m)) != nil {
		return 0, errors.New("redis: invalid corrupt book repair")
	}
	asof, err := liveTime(now)
	if err != nil {
		return 0, err
	}
	payload, err := marshaledMessage(durable)
	if err != nil {
		return 0, err
	}
	padded, raw := liveSequence(corrupt.Sequence)
	result, err := c.Client.Eval(ctx, repairCorruptLiveBook, liveKeys(c.Namespace, m), padded, corrupt.Raw, raw, asof, payload).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: repair corrupt live book: %w", err)
	}
	if result == int64(0) {
		return LiveRetry, nil
	}
	if result == int64(1) {
		return LiveApplied, nil
	}
	return 0, fmt.Errorf("redis: unexpected repair status %v", result)
}

// QuarantineLiveBook immediately makes the validated predecessor unavailable
// to public reads after a detected source/parity violation. The operation is
// fenced against races with healthy concurrent publishers.
func (c *Cache) QuarantineLiveBook(ctx context.Context, m marketregistry.Market, live *LiveBook) (LiveResult, error) {
	if c == nil || c.Client == nil || !c.Namespace.Valid() || live == nil || live.Snapshot == nil {
		return 0, ErrUnavailableView
	}
	seq, _ := liveSequence(live.Sequence)
	result, err := c.Client.Eval(ctx, quarantineLiveBook, liveKeys(c.Namespace, m), seq, live.Raw).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: quarantine live book: %w", err)
	}
	if result == int64(0) {
		return LiveRetry, nil
	}
	if result == int64(1) {
		return LiveApplied, nil
	}
	return 0, fmt.Errorf("redis: unexpected quarantine status %v", result)
}
