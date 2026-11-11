package readmodel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Stats preserve the exact (asOf-24h,asOf] nanosecond window and MATCHING
// sequence ordering. Only complete 1m buckets use durable rollups. The at
// most two partial boundary minutes are reduced by PostgreSQL exact NUMERIC
// aggregates, without returning individual trades or decoding protobufs.
const minute = time.Minute

type statsRow struct {
	firstSeq, firstOrdinal, firstPrice uint64
	lastSeq, lastOrdinal, lastPrice    uint64
	high, low                          uint64
	base, quote                        *big.Int
	count                              uint64
}

type statsTotals struct {
	firstSeq, firstOrdinal, firstPrice uint64
	lastSeq, lastOrdinal, lastPrice    uint64
	high, low                          uint64
	base, quote                        big.Int
	count                              uint64
}

func statsUint(s string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("readmodel: invalid uint64 statistic %q: %w", s, err)
	}
	return v, nil
}
func statsBig(s string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok || n.Sign() < 0 {
		return nil, errors.New("readmodel: invalid exact statistic")
	}
	return n, nil
}
func decodeStatsRow(v [11]string) (statsRow, error) {
	var r statsRow
	var err error
	nums := []*uint64{&r.firstSeq, &r.firstOrdinal, &r.firstPrice, &r.lastSeq, &r.lastOrdinal, &r.lastPrice, &r.high, &r.low, &r.count}
	for i, pos := range []int{0, 1, 2, 3, 4, 5, 6, 7, 10} {
		*nums[i], err = statsUint(v[pos])
		if err != nil {
			return r, err
		}
	}
	r.base, err = statsBig(v[8])
	if err != nil {
		return r, err
	}
	r.quote, err = statsBig(v[9])
	if err != nil {
		return r, err
	}
	if r.count == 0 || r.firstSeq == 0 || r.lastSeq == 0 || r.firstPrice == 0 || r.lastPrice == 0 || r.low == 0 || r.high < r.low || r.base.Sign() == 0 || r.quote.Sign() == 0 ||
		r.firstSeq > r.lastSeq || (r.firstSeq == r.lastSeq && r.firstOrdinal > r.lastOrdinal) {
		return r, errors.New("readmodel: invalid rollup invariants")
	}
	return r, nil
}
func earlier(aSeq, aOrd, bSeq, bOrd uint64) bool { return aSeq < bSeq || (aSeq == bSeq && aOrd < bOrd) }
func (t *statsTotals) add(r statsRow) error {
	if r.count > ^uint64(0)-t.count {
		return errors.New("readmodel: 24h trade count overflow")
	}
	if t.count == 0 || earlier(r.firstSeq, r.firstOrdinal, t.firstSeq, t.firstOrdinal) {
		t.firstSeq, t.firstOrdinal, t.firstPrice = r.firstSeq, r.firstOrdinal, r.firstPrice
	}
	if t.count == 0 || earlier(t.lastSeq, t.lastOrdinal, r.lastSeq, r.lastOrdinal) {
		t.lastSeq, t.lastOrdinal, t.lastPrice = r.lastSeq, r.lastOrdinal, r.lastPrice
	}
	if t.count == 0 || r.high > t.high {
		t.high = r.high
	}
	if t.count == 0 || r.low < t.low {
		t.low = r.low
	}
	t.base.Add(&t.base, r.base)
	t.quote.Add(&t.quote, r.quote)
	t.count += r.count
	return nil
}
func (t *statsTotals) message(market string, at time.Time, reference ...*uint64) *md.Stats24H {
	out := &md.Stats24H{MarketId: market, WindowStartExclusive: timestamppb.New(at.Add(-24 * time.Hour)),
		AsOfInclusive: timestamppb.New(at), BaseVolumeAtoms: t.base.String(), QuoteVolumeAtoms: t.quote.String(), PriceChangeBps: "", TradeCount: t.count}
	if t.count == 0 {
		return out
	}
	open, close, high, low := t.firstPrice, t.lastPrice, t.high, t.low
	out.OpenPriceUnits, out.ClosePriceUnits, out.HighPriceUnits, out.LowPriceUnits = &open, &close, &high, &low
	if len(reference) > 0 && reference[0] != nil && *reference[0] != 0 {
		diff := new(big.Int).Sub(new(big.Int).SetUint64(close), new(big.Int).SetUint64(*reference[0]))
		diff.Mul(diff, big.NewInt(10000))
		diff.Quo(diff, new(big.Int).SetUint64(*reference[0]))
		out.PriceChangeBps = diff.String()
	}
	return out
}

// aggregatePartial returns zero or one exact aggregate. Queries use the
// existing time-range index, so they never materialize an unbounded row set
// in Go. Open/close are selected by sequence, not recorded timestamp.
func aggregatePartial(ctx context.Context, tx *sql.Tx, topic string, id uint32, symbol string, after, through time.Time, excludeStart bool) (*statsRow, error) {
	// The lower boundary is exclusive, including when a timestamp equals the
	// 24h cutoff. The upper boundary can start exactly on a minute boundary.
	op := ">"
	if !excludeStart {
		op = ">="
	}
	lower, upper := postgres.NanosDecimal(after), postgres.NanosDecimal(through)
	if through.Before(after) || (through.Equal(after) && excludeStart) {
		return nil, nil
	}
	condition := `topic=$1 AND source_market_id=$2 AND recorded_at_ns ` + op + ` $3 AND recorded_at_ns <= $4`
	var count, base, quote string
	var high, low, minSymbol, maxSymbol sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT count(*)::text,coalesce(sum(base_atoms),0)::text,coalesce(sum(quote_atoms),0)::text,
      max(price_units)::text,min(price_units)::text,min(market_id),max(market_id) FROM market_data.public_trades WHERE `+condition,
		topic, int64(id), lower, upper).Scan(&count, &base, &quote, &high, &low, &minSymbol, &maxSymbol)
	if err != nil {
		return nil, err
	}
	if count == "0" {
		return nil, nil
	}
	if !high.Valid || !low.Valid || !minSymbol.Valid || !maxSymbol.Valid || minSymbol.String != symbol || maxSymbol.String != symbol {
		return nil, errors.New("readmodel: inconsistent boundary aggregate")
	}
	selectRow := func(desc bool) (string, string, string, error) {
		order := "ASC"
		if desc {
			order = "DESC"
		}
		var seq, ordinal, price string
		err := tx.QueryRowContext(ctx, `SELECT market_sequence::text,event_ordinal::text,price_units::text
          FROM market_data.public_trades WHERE `+condition+` ORDER BY market_sequence `+order+`,event_ordinal `+order+` LIMIT 1`,
			topic, int64(id), lower, upper).Scan(&seq, &ordinal, &price)
		return seq, ordinal, price, err
	}
	firstSeq, firstOrd, firstPrice, err := selectRow(false)
	if err != nil {
		return nil, err
	}
	lastSeq, lastOrd, lastPrice, err := selectRow(true)
	if err != nil {
		return nil, err
	}
	values := [11]string{firstSeq, firstOrd, firstPrice, lastSeq, lastOrd, lastPrice, high.String, low.String, base, quote, count}
	r, err := decodeStatsRow(values)
	return &r, err
}

func loadStats(ctx context.Context, tx *sql.Tx, topic string, id uint32, symbol string, asOf time.Time) (*md.Stats24H, error) {
	if symbol == "" {
		return nil, errors.New("readmodel: missing market symbol")
	}
	asOf = asOf.UTC()
	start := asOf.Add(-24 * time.Hour)
	fullStart, fullEnd := start.Truncate(minute).Add(minute), asOf.Truncate(minute)
	var totals statsTotals
	// Exactly 1439 full buckets in a normal 24h window; at most 1440 for
	// other future windows. This query is bounded independently of trade rate.
	rows, err := tx.QueryContext(ctx, `SELECT market_id,first_sequence::text,first_ordinal::text,first_price_units::text,
      last_sequence::text,last_ordinal::text,last_price_units::text,
      high_price_units::text,low_price_units::text,base_volume_atoms::text,quote_volume_atoms::text,trade_count::text
      FROM market_data.trade_minute_rollups
      WHERE topic=$1 AND source_market_id=$2 AND minute_start_ns >= $3 AND minute_start_ns < $4
      ORDER BY minute_start_ns`, topic, int64(id), postgres.NanosDecimal(fullStart), postgres.NanosDecimal(fullEnd))
	if err != nil {
		return nil, err
	}
	n := 0
	for rows.Next() {
		var v [11]string
		var rowSymbol string
		dest := make([]any, 12)
		dest[0] = &rowSymbol
		for i := range v {
			dest[i+1] = &v[i]
		}
		if err = rows.Scan(dest...); err != nil {
			break
		}
		if rowSymbol != symbol {
			err = errors.New("readmodel: minute rollup market identity mismatch")
			break
		}
		var r statsRow
		r, err = decodeStatsRow(v)
		if err != nil {
			break
		}
		err = totals.add(r)
		if err != nil {
			break
		}
		n++
		if n > 1440 {
			err = errors.New("readmodel: minute rollup bound exceeded")
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	// SQL exact aggregates for two partial minutes, never protobuf scans.
	left, err := aggregatePartial(ctx, tx, topic, id, symbol, start, fullStart.Add(-time.Nanosecond), true)
	if err != nil {
		return nil, err
	}
	if left != nil {
		if err := totals.add(*left); err != nil {
			return nil, err
		}
	}
	right, err := aggregatePartial(ctx, tx, topic, id, symbol, fullEnd, asOf, false)
	if err != nil {
		return nil, err
	}
	if right != nil {
		if err := totals.add(*right); err != nil {
			return nil, err
		}
	}
	// Most recent recorded execution at/before the strict 24h cutoff.
	// Never use the first trade after the cutoff as a fabricated reference.
	var rawReference string
	referenceErr := tx.QueryRowContext(ctx, `SELECT price_units::text FROM market_data.public_trades
        WHERE topic=$1 AND source_market_id=$2 AND recorded_at_ns <= $3
        ORDER BY recorded_at_ns DESC,market_sequence DESC,event_ordinal DESC LIMIT 1`, topic, int64(id), postgres.NanosDecimal(start)).Scan(&rawReference)
	if referenceErr != nil && !errors.Is(referenceErr, sql.ErrNoRows) {
		return nil, referenceErr
	}
	if errors.Is(referenceErr, sql.ErrNoRows) {
		return totals.message(symbol, asOf), nil
	}
	reference, e := statsUint(rawReference)
	if e != nil || reference == 0 {
		return nil, errors.New("readmodel: invalid 24h reference price")
	}
	return totals.message(symbol, asOf, &reference), nil
}

// LoadStats uses its own consistent read-only snapshot; cache misses no
// longer invoke Reader.Load's candles/trades/book bundle. The market's public
// projection must already exist, preventing zero-volume stats for unready IDs.
func (r *Reader) LoadStats(ctx context.Context, id uint32, asOf time.Time) (_ *md.Stats24H, err error) {
	if r == nil || r.DB == nil || r.Topic == "" {
		return nil, errors.New("readmodel: missing database or topic")
	}
	tx, err := r.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var symbol string
	err = tx.QueryRowContext(ctx, `SELECT market_id FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2`, r.Topic, int64(id)).Scan(&symbol)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotReady
	}
	if err != nil {
		return nil, err
	}
	result, err := loadStats(ctx, tx, r.Topic, id, symbol, asOf)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}
