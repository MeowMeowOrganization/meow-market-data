// Package readmodel reads a transactionally coherent public view from the
// nonfinancial PostgreSQL projection DB. Redis is a cache of this view.
package readmodel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/statistics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const RecentTradesLimit = 100
const RecentCandlesLimit = 200

var ErrNotReady = errors.New("readmodel: no durable projection for market")

type Bundle struct {
	SourceMarketID uint32
	AsOf           time.Time
	Snapshot       *md.BookSnapshot
	Ticker         *md.Ticker
	LatestTrades   []*md.TradeTick
	Candles        map[md.CandleInterval][]*md.Candle
	Stats          *md.Stats24H
}

type Reader struct {
	DB    *sql.DB
	Topic string
}

func parseTimeNanos(s string) (time.Time, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return time.Time{}, errors.New("readmodel: invalid recorded_at_ns")
	}
	// Euclidean division preserves negative timestamps as well.
	sec, nanos := new(big.Int).DivMod(n, big.NewInt(1_000_000_000), new(big.Int))
	if !sec.IsInt64() {
		return time.Time{}, errors.New("readmodel: timestamp outside Go time range")
	}
	return time.Unix(sec.Int64(), nanos.Int64()).UTC(), nil
}

func readTicks(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]*md.TradeTick, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var out []*md.TradeTick
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			break
		}
		t := new(md.TradeTick)
		if err = proto.Unmarshal(b, t); err != nil {
			break
		}
		out = append(out, t)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	return out, err
}

func (r *Reader) Load(ctx context.Context, id uint32, asOf time.Time) (_ *Bundle, err error) {
	if r == nil || r.DB == nil || r.Topic == "" {
		return nil, errors.New("readmodel: nil database")
	}
	asOf = asOf.UTC()
	tx, err := r.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var snapshotBytes []byte
	var recordedNS string
	err = tx.QueryRowContext(ctx, `SELECT snapshot,recorded_at_ns::text FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2`, r.Topic, int64(id)).Scan(&snapshotBytes, &recordedNS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotReady
	}
	if err != nil {
		return nil, err
	}
	snap := new(md.BookSnapshot)
	if err = proto.Unmarshal(snapshotBytes, snap); err != nil {
		return nil, fmt.Errorf("readmodel: corrupted book protobuf: %w", err)
	}
	if snap.GetSourceMarketId() != id || snap.GetMarketSequence() == 0 || snap.GetMarketId() == "" {
		return nil, errors.New("readmodel: inconsistent book")
	}
	updatedAt, err := parseTimeNanos(recordedNS)
	if err != nil {
		return nil, err
	}
	b := &Bundle{SourceMarketID: id, AsOf: asOf, Snapshot: snap, Candles: make(map[md.CandleInterval][]*md.Candle)}
	ticker := &md.Ticker{MarketId: snap.GetMarketId(), MarketSequence: snap.GetMarketSequence(), UpdatedAt: timestamppb.New(updatedAt)}
	if len(snap.GetBids()) != 0 {
		price := snap.GetBids()[0].GetPriceUnits()
		ticker.BestBidPriceUnits = &price
	}
	if len(snap.GetAsks()) != 0 {
		price := snap.GetAsks()[0].GetPriceUnits()
		ticker.BestAskPriceUnits = &price
	}
	// Latest price follows matching sequence, not wall-clock timestamp.
	recent, err := readTicks(ctx, tx, `SELECT payload FROM market_data.public_trades WHERE topic=$1 AND source_market_id=$2
        ORDER BY market_sequence DESC,event_ordinal DESC LIMIT $3`, r.Topic, int64(id), RecentTradesLimit)
	if err != nil {
		return nil, err
	}
	if len(recent) != 0 {
		price := recent[0].GetPriceUnits()
		ticker.LastPriceUnits = &price
	}
	b.LatestTrades = recent
	b.Ticker = ticker
	for _, interval := range statistics.SupportedIntervals() {
		rows, err := tx.QueryContext(ctx, `SELECT payload FROM market_data.candles
            WHERE topic=$1 AND source_market_id=$2 AND interval_id=$3 ORDER BY start_ns DESC LIMIT $4`,
			r.Topic, int64(id), int32(interval), RecentCandlesLimit)
		if err != nil {
			return nil, err
		}
		var candles []*md.Candle
		for rows.Next() {
			var payload []byte
			if err = rows.Scan(&payload); err != nil {
				break
			}
			candle := new(md.Candle)
			if err = proto.Unmarshal(payload, candle); err != nil {
				break
			}
			candles = append(candles, candle)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return nil, err
		}
		b.Candles[interval] = candles
	}
	b.Stats, err = loadStats(ctx, tx, r.Topic, id, snap.GetMarketId(), asOf)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return b, nil
}
