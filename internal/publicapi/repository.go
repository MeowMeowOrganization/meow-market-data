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
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bookcheck"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	rediscache "github.com/MeowMeowOrganization/meow-market-data/internal/storage/redis"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var ErrNotReady = readmodel.ErrNotReady
var ErrCorrupt = errors.New("publicapi: inconsistent durable projection")

type Repository struct {
	DB             *sql.DB
	Topic          string
	Cache          *redis.Client // optional: all methods have a durable PG fallback
	CacheNamespace rediscache.Namespace
}

func (r *Repository) check() error {
	if r == nil || r.DB == nil || r.Topic == "" {
		return errors.New("publicapi: missing database or source topic")
	}
	return nil
}

// Cached payload and its sequence are read in one Redis Lua invocation;
// a concurrent refresh cannot mix metadata with another version's payload.
func (r *Repository) cacheView(ctx context.Context, m marketregistry.Market, now time.Time, suffixes ...string) (rediscache.View, error) {
	if r == nil || r.Cache == nil {
		return rediscache.View{}, rediscache.ErrUnavailableView
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	return rediscache.ReadFresh(cacheCtx, r.Cache, r.CacheNamespace, m.SourceID, m.DefinitionVersion, now, suffixes...)
}
func validateBook(s *md.BookSnapshot, m marketregistry.Market) error {
	if err := bookcheck.Validate(s, bookcheck.Expected{SourceMarketID: m.SourceID, MarketID: m.Symbol,
		DefinitionVersion: m.DefinitionVersion, Depth: m.Depth, PriceTickUnits: m.PriceTickUnits,
		QuantityStepBaseAtoms: m.QuantityStepAtoms}); err != nil {
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

// Book prefers the independently sequence-fenced live L2 projection. The
// periodic full bundle may trail this book and must never mask live failures.
// If Redis is missing, corrupt or behind its health check, use durable PG.
func (r *Repository) Book(ctx context.Context, m marketregistry.Market, depth uint32) (*md.BookSnapshot, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	if depth == 0 || depth > m.Depth {
		return nil, errors.New("publicapi: invalid book depth")
	}
	if r.Cache != nil {
		cacheCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		live, err := rediscache.LoadLiveBook(cacheCtx, r.Cache, r.CacheNamespace, m, time.Now().UTC(), true)
		cancel()
		if err == nil {
			return clipDepth(live.Snapshot, depth), nil
		}
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
	if limit <= readmodel.RecentTradesLimit {
		view, err := r.cacheView(ctx, m, time.Now().UTC(), "trades:latest")
		if err == nil {
			var raw []json.RawMessage
			if json.Unmarshal([]byte(view.Values[0]), &raw) == nil && len(raw) > 0 && len(raw) <= readmodel.RecentTradesLimit {
				ticks := make([]*md.TradeTick, 0, len(raw))
				valid := true
				var previousSequence, previousOrdinal uint64
				for i, v := range raw {
					t := new(md.TradeTick)
					if protojson.Unmarshal(v, t) != nil || t.GetSourceMarketId() != m.SourceID || t.GetMarketId() != m.Symbol ||
						t.GetTradeId() == "" || t.GetMatchSequence() == 0 || t.GetMatchSequence() > view.Sequence ||
						t.GetPriceUnits() == 0 || t.GetBaseQuantityAtoms() == 0 || t.GetQuoteQuantityAtoms() == 0 ||
						t.GetRecordedAt() == nil || t.GetRecordedAt().CheckValid() != nil ||
						(i > 0 && (t.GetMatchSequence() > previousSequence ||
							(t.GetMatchSequence() == previousSequence && t.GetEventOrdinal() >= previousOrdinal))) {
						valid = false
						break
					}
					previousSequence, previousOrdinal = t.GetMatchSequence(), t.GetEventOrdinal()
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
	view, err := r.cacheView(ctx, m, now, "ticker", "book:top")
	if err == nil {
		ticker, snapshot := new(md.Ticker), new(md.BookSnapshot)
		if protojson.Unmarshal([]byte(view.Values[0]), ticker) == nil &&
			protojson.Unmarshal([]byte(view.Values[1]), snapshot) == nil &&
			validateBook(snapshot, m) == nil && snapshot.GetMarketSequence() == view.Sequence &&
			bookcheck.ValidateTicker(ticker, snapshot) == nil {
			return ticker, nil
		}
	}
	b, err := (&readmodel.Reader{DB: r.DB, Topic: r.Topic}).Load(ctx, m.SourceID, now)
	if err != nil {
		return nil, err
	}
	return b.Ticker, nil
}
func (r *Repository) Stats(ctx context.Context, m marketregistry.Market, now time.Time) (*md.Stats24H, error) {
	view, err := r.cacheView(ctx, m, now, "stats:24h")
	if err == nil {
		stats := new(md.Stats24H)
		if protojson.Unmarshal([]byte(view.Values[0]), stats) == nil && bookcheck.ValidateStats(stats, m.Symbol, now) == nil {
			return stats, nil
		}
	}
	return (&readmodel.Reader{DB: r.DB, Topic: r.Topic}).LoadStats(ctx, m.SourceID, now)
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
