// Package redis maintains disposable market-data read models. Full bundles are
// published atomically and fenced by exact unsigned decimal sequences; Redis
// must never participate in durable matching-result acknowledgement.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Script checks (sequence, as-of) as padded decimal ASCII. Lua tonumber loses
// precision for uint64 above 2^53. KEYS share the market ID hash tag.
// A stale update is reported to the caller, never silently treated as applied.
const replaceBundle = `
local oldseq = redis.call('HGET', KEYS[1], 'sequence_sort')
local oldasof = redis.call('HGET', KEYS[1], 'asof_sort')
-- Corrupt or future-dated refresh timestamps must not lock a sequence forever.
if oldasof and (#oldasof ~= 30 or string.find(oldasof, '[^0-9]') or oldasof > ARGV[3]) then
  oldasof = nil
end
if oldseq and (oldseq > ARGV[1] or (oldseq == ARGV[1] and oldasof and oldasof > ARGV[2])) then
  return 0
end
for i=2,#KEYS do
  redis.call('SET', KEYS[i], ARGV[i+2])
end
redis.call('HSET', KEYS[1], 'sequence_sort', ARGV[1], 'asof_sort', ARGV[2], 'sequence', ARGV[#KEYS+3])
return 1
`

type PublishResult = readmodel.PublishResult

const (
	PublishApplied = readmodel.PublishApplied
	PublishStale   = readmodel.PublishStale
)

type Cache struct {
	Client    *goredis.Client
	Namespace Namespace
}

func New(url, topic, epoch string) (*Cache, error) {
	if url == "" {
		return nil, errors.New("redis: URL required")
	}
	namespace, err := NewNamespace(topic, epoch)
	if err != nil {
		return nil, err
	}
	opts, err := goredis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return &Cache{Client: goredis.NewClient(opts), Namespace: namespace}, nil
}
func (c *Cache) Close() error { return c.Client.Close() }

func padDecimal(s string, width int) (string, error) {
	if s == "" || len(s) > width {
		return "", errors.New("redis: decimal width out of range")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return "", errors.New("redis: invalid decimal")
		}
	}
	return strings.Repeat("0", width-len(s)) + s, nil
}
func marshaledMessage(m proto.Message) (string, error) {
	v, err := protojson.Marshal(m)
	return string(v), err
}
func marshaledList[T proto.Message](items []T) (string, error) {
	list := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		b, err := protojson.Marshal(item)
		if err != nil {
			return "", err
		}
		list = append(list, b)
	}
	b, err := json.Marshal(list)
	return string(b), err
}

func (c *Cache) Publish(ctx context.Context, b *readmodel.Bundle) (PublishResult, error) {
	if c == nil || c.Client == nil || !c.Namespace.Valid() {
		return 0, errors.New("redis: missing client or source namespace")
	}
	if err := ValidateBundle(b); err != nil {
		return 0, err
	}
	if b.AsOf.After(time.Now().UTC().Add(5 * time.Second)) {
		return 0, fmt.Errorf("%w: future refresh time", ErrInvalidBundle)
	}
	seq := strconv.FormatUint(b.Snapshot.GetMarketSequence(), 10)
	seqSort, err := padDecimal(seq, 20)
	if err != nil {
		return 0, err
	}
	asof := postgres.NanosDecimal(b.AsOf)
	asofSort, err := padDecimal(asof, 30)
	if err != nil {
		return 0, err
	}
	prefix := c.Namespace.MarketPrefix(b.SourceMarketID, b.Snapshot.GetMarketDefinitionVersion())
	keys := []string{prefix + "state", prefix + "book:top", prefix + "ticker", prefix + "trades:latest", prefix + "stats:24h"}
	upper, err := padDecimal(postgres.NanosDecimal(time.Now().UTC().Add(5*time.Second)), 30)
	if err != nil {
		return 0, err
	}
	args := []any{seqSort, asofSort, upper}
	snapshot, err := marshaledMessage(b.Snapshot)
	if err != nil {
		return 0, err
	}
	ticker, err := marshaledMessage(b.Ticker)
	if err != nil {
		return 0, err
	}
	trades, err := marshaledList(b.LatestTrades)
	if err != nil {
		return 0, err
	}
	stats, err := marshaledMessage(b.Stats)
	if err != nil {
		return 0, err
	}
	args = append(args, snapshot, ticker, trades, stats)
	for _, interval := range []struct {
		kind   md.CandleInterval
		suffix string
	}{
		{md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, "1m"},
		{md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES, "5m"},
		{md.CandleInterval_CANDLE_INTERVAL_FIFTEEN_MINUTES, "15m"},
		{md.CandleInterval_CANDLE_INTERVAL_ONE_HOUR, "1h"},
		{md.CandleInterval_CANDLE_INTERVAL_ONE_DAY, "1d"},
	} {
		keys = append(keys, prefix+"candles:"+interval.suffix)
		value, err := marshaledList(b.Candles[interval.kind])
		if err != nil {
			return 0, err
		}
		args = append(args, value)
	}
	args = append(args, seq) // raw sequence; Lua never parses it as a number
	result, err := c.Client.Eval(ctx, replaceBundle, keys, args...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: atomic view update: %w", err)
	}
	switch result {
	case int64(0):
		return PublishStale, nil
	case int64(1):
		return PublishApplied, nil
	default:
		return 0, fmt.Errorf("redis: unexpected Lua result %v", result)
	}
}
