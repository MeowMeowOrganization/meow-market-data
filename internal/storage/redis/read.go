package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	goredis "github.com/redis/go-redis/v9"
)

var ErrUnavailableView = errors.New("redis: no fresh complete cache view")

// A single Lua call reads metadata and all requested values without allowing a
// concurrent bundle publication between the freshness and payload reads.
// It also rejects missing/partially evicted keys and future-dated refreshes.
const loadFreshView = `
local seq = redis.call('HGET', KEYS[1], 'sequence')
local padded = redis.call('HGET', KEYS[1], 'sequence_sort')
local asof = redis.call('HGET', KEYS[1], 'asof_sort')
if not seq or not padded or not asof or #padded ~= 20 or #asof ~= 30 or
   string.find(seq, '[^0-9]') or string.find(padded, '[^0-9]') or
   string.find(asof, '[^0-9]') or padded ~= string.rep('0', 20 - #seq) .. seq or
   asof < ARGV[1] or asof > ARGV[2] then
  return nil
end
local result = { seq }
for i=2,#KEYS do
  local value = redis.call('GET', KEYS[i])
  if not value then return nil end
  result[#result+1] = value
end
return result
`

type View struct {
	Sequence uint64
	Values   []string
}

// ReadFresh accepts an intentionally eventual view but not a partially read
// one. Source lag is a separate operational signal (not inferred from as-of).
func ReadFresh(ctx context.Context, client *goredis.Client, namespace Namespace,
	marketID uint32, definitionVersion uint64, now time.Time, suffixes ...string) (View, error) {
	if client == nil || !namespace.Valid() || definitionVersion == 0 || len(suffixes) == 0 || now.Before(time.Unix(0, 0)) {
		return View{}, ErrUnavailableView
	}
	prefix := namespace.MarketPrefix(marketID, definitionVersion)
	keys := make([]string, 0, 1+len(suffixes))
	keys = append(keys, prefix+"state")
	for _, suffix := range suffixes {
		keys = append(keys, prefix+suffix)
	}
	lower, err := padDecimal(postgres.NanosDecimal(now.UTC().Add(-20*time.Second)), 30)
	if err != nil {
		return View{}, err
	}
	upper, err := padDecimal(postgres.NanosDecimal(now.UTC().Add(5*time.Second)), 30)
	if err != nil {
		return View{}, err
	}
	value, err := client.Eval(ctx, loadFreshView, keys, lower, upper).Result()
	if errors.Is(err, goredis.Nil) || value == nil {
		return View{}, ErrUnavailableView
	}
	if err != nil {
		return View{}, fmt.Errorf("redis: atomic view read: %w", err)
	}
	items, ok := value.([]interface{})
	if !ok || len(items) != len(suffixes)+1 {
		return View{}, ErrUnavailableView
	}
	seqString, ok := items[0].(string)
	if !ok {
		return View{}, ErrUnavailableView
	}
	seq, err := strconv.ParseUint(seqString, 10, 64)
	if err != nil || seq == 0 {
		return View{}, ErrUnavailableView
	}
	view := View{Sequence: seq, Values: make([]string, len(suffixes))}
	for i := range view.Values {
		str, ok := items[i+1].(string)
		if !ok {
			return View{}, ErrUnavailableView
		}
		view.Values[i] = str
	}
	return view, nil
}
