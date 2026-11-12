package stream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-contracts/tradeid"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bookcheck"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TickerSource provides an authoritative, already coalesced latest-state
// observation. It does NOT read the periodic Redis soft cache: those values
// can lag an independently committed book or execution projection.
// This optional capability keeps the established Source interface compatible
// with the book/trade/WebSocket tests and other transports.
type TickerSource interface {
	LatestTicker(context.Context, marketregistry.Market) (*md.Ticker, error)
}

// tickerRecordedAt decodes exact PostgreSQL NUMERIC nanoseconds, including
// pre-epoch values. No float conversion is ever used.
func tickerRecordedAt(raw string) (*timestamppb.Timestamp, error) {
	ns, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		return nil, ErrSequenceGap
	}
	sec, nsec := new(big.Int), new(big.Int)
	sec.DivMod(ns, big.NewInt(1_000_000_000), nsec)
	if !sec.IsInt64() {
		return nil, ErrSequenceGap
	}
	ts := timestamppb.New(time.Unix(sec.Int64(), nsec.Int64()).UTC())
	if err := ts.CheckValid(); err != nil {
		return nil, fmt.Errorf("stream: invalid durable ticker timestamp: %w", err)
	}
	return ts, nil
}

// LatestTicker reads the durable book snapshot, its recorded-at timestamp,
// and the last MATCHING-ORDERED execution under ONE REPEATABLE READ snapshot.
// It is a small indexed PG read, unlike readmodel.Reader.Load which also
// calculates sliding stats and materializes candles on every invocation.
// Absolute book sequence advances without trades, and optional prices remain
// absent until there is a real resting level / execution.
func (s *PostgresSource) LatestTicker(ctx context.Context, m marketregistry.Market) (_ *md.Ticker, err error) {
	if err = s.check(); err != nil {
		return nil, err
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var snapshotBytes []byte
	var recordedAt string
	err = tx.QueryRowContext(ctx, `SELECT CASE WHEN octet_length(snapshot) <= $3 THEN snapshot ELSE NULL END,recorded_at_ns::text
        FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2`,
		s.Topic, int64(m.SourceID), MaxSnapshotBytes).Scan(&snapshotBytes, &recordedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotReady
	}
	if err != nil {
		return nil, err
	}
	if snapshotBytes == nil {
		return nil, ErrPayloadTooLarge
	}
	snap := new(md.BookSnapshot)
	if err = proto.Unmarshal(snapshotBytes, snap); err != nil {
		return nil, fmt.Errorf("stream: invalid ticker book protobuf: %w", err)
	}
	if err = bookcheck.Validate(snap, bookcheck.Expected{
		SourceMarketID: m.SourceID, MarketID: m.Symbol, DefinitionVersion: m.DefinitionVersion,
		Depth: m.Depth, PriceTickUnits: m.PriceTickUnits, QuantityStepBaseAtoms: m.QuantityStepAtoms,
	}); err != nil {
		return nil, fmt.Errorf("stream: invalid ticker book state: %w", err)
	}
	updatedAt, err := tickerRecordedAt(recordedAt)
	if err != nil {
		return nil, err
	}
	ticker := &md.Ticker{MarketId: m.Symbol, MarketSequence: snap.GetMarketSequence(), UpdatedAt: updatedAt}
	if levels := snap.GetBids(); len(levels) != 0 {
		price := levels[0].GetPriceUnits()
		ticker.BestBidPriceUnits = &price
	}
	if levels := snap.GetAsks(); len(levels) != 0 {
		price := levels[0].GetPriceUnits()
		ticker.BestAskPriceUnits = &price
	}
	// There is at most one last execution. A missing row represents an
	// untraded market; it must not fabricate a zero last price.
	var tradeBytes []byte
	err = tx.QueryRowContext(ctx, `SELECT CASE WHEN octet_length(payload) <= $3 THEN payload ELSE NULL END
        FROM market_data.public_trades WHERE topic=$1 AND source_market_id=$2
        ORDER BY market_sequence DESC,event_ordinal DESC LIMIT 1`,
		s.Topic, int64(m.SourceID), MaxFeedBytes).Scan(&tradeBytes)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err == nil {
		if tradeBytes == nil {
			return nil, ErrPayloadTooLarge
		}
		trade := new(md.TradeTick)
		if err = proto.Unmarshal(tradeBytes, trade); err != nil {
			return nil, fmt.Errorf("stream: invalid ticker trade protobuf: %w", err)
		}
		id, seq, ord, parseErr := tradeid.Parse(trade.GetTradeId())
		if parseErr != nil || id != m.SourceID || seq != trade.GetMatchSequence() || ord != trade.GetEventOrdinal() ||
			trade.GetMatchSequence() == 0 || trade.GetMatchSequence() > snap.GetMarketSequence() ||
			trade.GetMarketId() != m.Symbol || trade.GetSourceMarketId() != m.SourceID ||
			trade.GetMarketDefinitionVersion() != m.DefinitionVersion || trade.GetPriceUnits() == 0 {
			return nil, ErrSequenceGap
		}
		price := trade.GetPriceUnits()
		ticker.LastPriceUnits = &price
	}
	if err := bookcheck.ValidateTicker(ticker, snap); err != nil {
		return nil, fmt.Errorf("stream: inconsistent durable ticker: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ticker, nil
}
