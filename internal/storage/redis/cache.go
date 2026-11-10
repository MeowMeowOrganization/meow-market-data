// Package redis maintains disposable market-data read models. The entire
// bundle is replaced in ONE Lua script and fenced by uint64-safe lexical
// sequence numbers; no Redis floating-point score is ever used.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Script checks the monotonic pair (market sequence, refresh time) as padded
// decimal ASCII. Redis Lua tonumber would silently round uint64 > 2^53.
// All keys share the same Redis Cluster hash tag {marketID}.
const replaceBundle = `
local oldseq = redis.call('HGET', KEYS[1], 'sequence_sort')
local oldasof = redis.call('HGET', KEYS[1], 'asof_sort')
if oldseq and (oldseq > ARGV[1] or (oldseq == ARGV[1] and oldasof and oldasof > ARGV[2])) then
  return 0
end
for i=2,#KEYS do
  redis.call('SET', KEYS[i], ARGV[i+1])
end
redis.call('HSET', KEYS[1], 'sequence_sort', ARGV[1], 'asof_sort', ARGV[2], 'sequence', ARGV[#KEYS+2])
return 1
`

type Cache struct{ Client *redis.Client }

func New(url string) (*Cache, error) {
	if url == "" {
		return nil, errors.New("redis: URL required")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return &Cache{Client: redis.NewClient(opts)}, nil
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

func (c *Cache) Publish(ctx context.Context, b *readmodel.Bundle) error {
	if c == nil || c.Client == nil || b == nil || b.Snapshot == nil || b.Ticker == nil || b.Stats == nil || b.SourceMarketID != b.Snapshot.GetSourceMarketId() {
		return errors.New("redis: incomplete read model")
	}
	seq := strconv.FormatUint(b.Snapshot.GetMarketSequence(), 10)
	seqSort, err := padDecimal(seq, 20)
	if err != nil {
		return err
	}
	asof := postgres.NanosDecimal(b.AsOf)
	asofSort, err := padDecimal(asof, 30) // positive Unix time for current operation
	if err != nil {
		return err
	}
	market := strconv.FormatUint(uint64(b.SourceMarketID), 10)
	prefix := "md:v1:m:{" + market + "}:"
	keys := []string{prefix + "state", prefix + "book:top", prefix + "ticker", prefix + "trades:latest", prefix + "stats:24h"}
	args := []any{seqSort, asofSort}
	snapshot, err := marshaledMessage(b.Snapshot)
	if err != nil {
		return err
	}
	ticker, err := marshaledMessage(b.Ticker)
	if err != nil {
		return err
	}
	trades, err := marshaledList(b.LatestTrades)
	if err != nil {
		return err
	}
	stats, err := marshaledMessage(b.Stats)
	if err != nil {
		return err
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
			return err
		}
		args = append(args, value)
	}
	// The raw sequence is the last argument. No Lua numeric parsing occurs.
	args = append(args, seq)
	result, err := c.Client.Eval(ctx, replaceBundle, keys, args...).Result()
	if err != nil {
		return fmt.Errorf("redis: atomic view update: %w", err)
	}
	if result != int64(0) && result != int64(1) {
		return fmt.Errorf("redis: unexpected Lua result %v", result)
	}
	return nil
}
