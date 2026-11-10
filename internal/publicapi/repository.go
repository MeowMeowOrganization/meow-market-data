// Package publicapi serves only public market projections. PostgreSQL is the
// durable query source; Redis is an optional disposable, eventually consistent
// fast path for small current-state responses.
package publicapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var ErrNotReady = readmodel.ErrNotReady
var ErrCorrupt = errors.New("publicapi: inconsistent durable projection")

type Repository struct {
	DB    *sql.DB
	Topic string
	Cache *redis.Client // optional: all methods have a durable PG fallback
}

func (r *Repository) check() error {
	if r == nil || r.DB == nil || r.Topic == "" {
		return errors.New("publicapi: missing database or source topic")
	}
	return nil
}
func cacheKey(m marketregistry.Market, suffix string) string {
	return fmt.Sprintf("md:v1:m:{%d}:%s", m.SourceID, suffix)
}
func (r *Repository) cacheFresh(ctx context.Context, m marketregistry.Market, now time.Time) bool {
	if r.Cache == nil {
		return false
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	recorded, err := r.Cache.HGet(cacheCtx, cacheKey(m, "state"), "asof_sort").Result()
	if err != nil || len(recorded) != 30 {
		return false
	}
	cutoff := postgres.NanosDecimal(now.Add(-20 * time.Second))
	if len(cutoff) > 30 {
		return false
	}
	return recorded >= strings.Repeat("0", 30-len(cutoff))+cutoff
}
func (r *Repository) cached(ctx context.Context, m marketregistry.Market, suffix string, msg proto.Message) bool {
	if !r.cacheFresh(ctx, m, time.Now().UTC()) {
		return false
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	s, err := r.Cache.Get(cacheCtx, cacheKey(m, suffix)).Result()
	if err != nil {
		return false
	}
	return protojson.Unmarshal([]byte(s), msg) == nil
}
func validateBook(s *md.BookSnapshot, m marketregistry.Market) error {
	if s == nil || s.GetSourceMarketId() != m.SourceID || s.GetMarketId() != m.Symbol ||
		s.GetDepth() != m.Depth || s.GetMarketDefinitionVersion() != m.DefinitionVersion || s.GetMarketSequence() == 0 {
		return ErrCorrupt
	}
	return nil
}
func (r *Repository) Snapshot(ctx context.Context, m marketregistry.Market) (*md.BookSnapshot, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	var b []byte
	err := r.DB.QueryRowContext(ctx, `SELECT snapshot FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2`, r.Topic, int64(m.SourceID)).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotReady
	}
	if err != nil {
		return nil, err
	}
	snap := new(md.BookSnapshot)
	if err := proto.Unmarshal(b, snap); err != nil {
		return nil, fmt.Errorf("%w: snapshot decode: %v", ErrCorrupt, err)
	}
	return snap, validateBook(snap, m)
}
func (r *Repository) Book(ctx context.Context, m marketregistry.Market, depth uint32) (*md.BookSnapshot, error) {
	if depth == 0 || depth > m.Depth {
		return nil, errors.New("publicapi: invalid book depth")
	}
	cached := new(md.BookSnapshot)
	if r.cached(ctx, m, "book:top", cached) && validateBook(cached, m) == nil {
		return clipDepth(cached, depth), nil
	}
	durable, err := r.Snapshot(ctx, m)
	if err != nil {
		return nil, err
	}
	return clipDepth(durable, depth), nil
}
func clipDepth(s *md.BookSnapshot, depth uint32) *md.BookSnapshot {
	c := proto.Clone(s).(*md.BookSnapshot)
	c.Depth = depth
	if len(c.Bids) > int(depth) {
		c.Bids = c.Bids[:int(depth)]
	}
	if len(c.Asks) > int(depth) {
		c.Asks = c.Asks[:int(depth)]
	}
	return c
}
func (r *Repository) Trades(ctx context.Context, m marketregistry.Market, limit uint32) ([]*md.TradeTick, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	if limit == 0 || limit > 1000 {
		return nil, errors.New("publicapi: invalid trade limit")
	}
	if limit <= readmodel.RecentTradesLimit && r.cacheFresh(ctx, m, time.Now().UTC()) {
		cacheCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		s, err := r.Cache.Get(cacheCtx, cacheKey(m, "trades:latest")).Result()
		cancel()
		if err == nil {
			var raw []json.RawMessage
			if json.Unmarshal([]byte(s), &raw) == nil && len(raw) > 0 {
				ticks := make([]*md.TradeTick, 0, len(raw))
				valid := true
				for _, v := range raw {
					t := new(md.TradeTick)
					if protojson.Unmarshal(v, t) != nil || t.GetSourceMarketId() != m.SourceID || t.GetMarketId() != m.Symbol {
						valid = false
						break
					}
					ticks = append(ticks, t)
				}
				if valid {
					if len(ticks) > int(limit) {
						ticks = ticks[:int(limit)]
					}
					return ticks, nil
				}
			}
		}
	}
	rows, err := r.DB.QueryContext(ctx, `SELECT payload FROM market_data.public_trades
        WHERE topic=$1 AND source_market_id=$2 ORDER BY market_sequence DESC,event_ordinal DESC LIMIT $3`, r.Topic, int64(m.SourceID), int64(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*md.TradeTick
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		t := new(md.TradeTick)
		if err := proto.Unmarshal(b, t); err != nil {
			return nil, fmt.Errorf("%w: trade decode: %v", ErrCorrupt, err)
		}
		if t.GetSourceMarketId() != m.SourceID || t.GetMarketId() != m.Symbol {
			return nil, ErrCorrupt
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
func (r *Repository) Ticker(ctx context.Context, m marketregistry.Market, now time.Time) (*md.Ticker, error) {
	cached := new(md.Ticker)
	if r.cached(ctx, m, "ticker", cached) && cached.GetMarketId() == m.Symbol && cached.GetMarketSequence() > 0 {
		return cached, nil
	}
	b, err := (&readmodel.Reader{DB: r.DB, Topic: r.Topic}).Load(ctx, m.SourceID, now)
	if err != nil {
		return nil, err
	}
	return b.Ticker, nil
}
func (r *Repository) Stats(ctx context.Context, m marketregistry.Market, now time.Time) (*md.Stats24H, error) {
	cached := new(md.Stats24H)
	if r.cached(ctx, m, "stats:24h", cached) && cached.GetMarketId() == m.Symbol && cached.GetAsOfInclusive() != nil {
		return cached, nil
	}
	b, err := (&readmodel.Reader{DB: r.DB, Topic: r.Topic}).Load(ctx, m.SourceID, now)
	if err != nil {
		return nil, err
	}
	return b.Stats, nil
}

// Candles returns bounded keyset pages ordered oldest-first. A candle's key is
// immutable, but its revision may change between pages; clients must reconcile
// revisions and should not interpret pages as an MVCC snapshot across requests.
func (r *Repository) Candles(ctx context.Context, m marketregistry.Market, interval md.CandleInterval,
	start, end time.Time, after string, limit uint32) ([]*md.Candle, bool, error) {
	if err := r.check(); err != nil {
		return nil, false, err
	}
	if _, err := r.Snapshot(ctx, m); err != nil {
		return nil, false, err
	}
	if limit == 0 || limit > 500 {
		return nil, false, errors.New("publicapi: invalid candle limit")
	}
	startNS, endNS := postgres.NanosDecimal(start), postgres.NanosDecimal(end)
	resume := after != ""
	if !resume {
		after = startNS
	}
	// Page 1 includes start_ns; subsequent pages exclude the last key.
	comparator := "start_ns >= $6"
	if resume {
		comparator = "start_ns > $6"
	}
	query := `SELECT payload FROM market_data.candles
        WHERE topic=$1 AND source_market_id=$2 AND interval_id=$3
          AND start_ns >= $4 AND start_ns < $5 AND ` + comparator + `
        ORDER BY start_ns ASC LIMIT $7`
	rows, err := r.DB.QueryContext(ctx, query, r.Topic, int64(m.SourceID), int32(interval), startNS, endNS, after, int64(limit)+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	out := make([]*md.Candle, 0, limit+1)
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, false, err
		}
		c := new(md.Candle)
		if err := proto.Unmarshal(data, c); err != nil {
			return nil, false, fmt.Errorf("%w: candle decode: %v", ErrCorrupt, err)
		}
		if c.GetMarketId() != m.Symbol || c.GetInterval() != interval || c.GetStartTime() == nil || c.GetStartTime().CheckValid() != nil {
			return nil, false, ErrCorrupt
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(out) > int(limit)
	if hasMore {
		out = out[:limit]
	}
	return out, hasMore, nil
}
