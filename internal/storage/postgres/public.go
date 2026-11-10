package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matching "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/statistics"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"google.golang.org/protobuf/proto"
)

// NanosDecimal preserves full Protobuf timestamp nanosecond precision even
// when PostgreSQL TIMESTAMPTZ would round to microseconds. BIGINT cannot hold
// the full protobuf date domain, hence a bounded NUMERIC(30,0) column.
func NanosDecimal(t time.Time) string {
	n := new(big.Int).Mul(big.NewInt(t.Unix()), big.NewInt(1_000_000_000))
	n.Add(n, big.NewInt(int64(t.Nanosecond())))
	return n.String()
}

func (s *Store) savePublicProjection(ctx context.Context, tx *sql.Tx, topic string, p recovery.DurableProjection) error {
	if p.Snapshot == nil || p.Snapshot.GetSourceMarketId() != p.MarketID || p.Snapshot.GetDepth() == 0 || len(p.Envelope) == 0 {
		return errors.New("postgres public: missing or mismatched snapshot/envelope")
	}
	var envelope matching.CommittedMatchingResult
	if err := proto.Unmarshal(p.Envelope, &envelope); err != nil {
		return fmt.Errorf("postgres public: decode envelope: %w", err)
	}
	if envelope.GetResult() == nil || envelope.GetRecordedAt() == nil || envelope.GetRecordedAt().CheckValid() != nil ||
		envelope.GetResult().GetCommand() == nil || envelope.GetResult().GetCommand().GetMarketId() != p.MarketID ||
		envelope.GetResult().GetMarketSequence() != p.Snapshot.GetMarketSequence() ||
		envelope.GetMarketDefinitionVersion() != p.Snapshot.GetMarketDefinitionVersion() {
		return errors.New("postgres public: result does not match book snapshot")
	}
	if p.Delta == nil {
		if len(p.Trades) != 0 {
			return errors.New("postgres public: duplicate cannot contain trades")
		}
		return nil // exact retry of latest matcher result at a new Kafka offset
	}
	if p.Delta.GetMarketSequence() != p.Snapshot.GetMarketSequence() || p.Delta.GetMarketDefinitionVersion() != p.Snapshot.GetMarketDefinitionVersion() ||
		p.Delta.GetDepth() != p.Snapshot.GetDepth() || p.Snapshot.GetMarketId() == "" || p.Delta.GetMarketId() != p.Snapshot.GetMarketId() ||
		p.Delta.GetSourceMarketId() != p.MarketID {
		return errors.New("postgres public: delta/snapshot mismatch")
	}
	snapshot, err := (proto.MarshalOptions{Deterministic: true}).Marshal(p.Snapshot)
	if err != nil {
		return err
	}
	delta, err := (proto.MarshalOptions{Deterministic: true}).Marshal(p.Delta)
	if err != nil {
		return err
	}
	var previous string
	err = tx.QueryRowContext(ctx, `SELECT market_sequence::text FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2 FOR UPDATE`, topic, int64(p.MarketID)).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		if p.Delta.GetPreviousMarketSequence() != 0 {
			return errors.New("postgres public: missing predecessor book")
		}
	} else if previous != fmt.Sprint(p.Delta.GetPreviousMarketSequence()) {
		return fmt.Errorf("postgres public: current book sequence %s does not match delta predecessor %d", previous, p.Delta.GetPreviousMarketSequence())
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO market_data.public_books
        (topic,source_market_id,market_id,market_sequence,market_definition_version,depth,recorded_at_ns,snapshot)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (topic,source_market_id) DO UPDATE SET market_id=EXCLUDED.market_id,
        market_sequence=EXCLUDED.market_sequence, market_definition_version=EXCLUDED.market_definition_version,
        depth=EXCLUDED.depth, recorded_at_ns=EXCLUDED.recorded_at_ns, snapshot=EXCLUDED.snapshot, updated_at=now()`,
		topic, int64(p.MarketID), p.Snapshot.GetMarketId(), fmt.Sprint(p.Snapshot.GetMarketSequence()), fmt.Sprint(p.Snapshot.GetMarketDefinitionVersion()),
		int64(p.Snapshot.GetDepth()), NanosDecimal(envelope.GetRecordedAt().AsTime()), snapshot); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO market_data.book_deltas(topic,source_market_id,market_sequence,payload) VALUES($1,$2,$3,$4)`,
		topic, int64(p.MarketID), fmt.Sprint(p.Delta.GetMarketSequence()), delta); err != nil {
		return err
	}

	// Per-result writes are bounded by the matcher event vector. Build the
	// affected UTC intervals once, and recompute AFTER all trades are inserted.
	type intervalKey struct {
		interval md.CandleInterval
		start    time.Time
	}
	affected := make(map[intervalKey]struct{})
	for _, trade := range p.Trades {
		if trade == nil || trade.GetSourceMarketId() != p.MarketID || trade.GetMarketId() != p.Snapshot.GetMarketId() ||
			trade.GetMatchSequence() != p.Delta.GetMarketSequence() || trade.GetRecordedAt() == nil || trade.GetRecordedAt().CheckValid() != nil ||
			trade.GetPriceUnits() == 0 || trade.GetBaseQuantityAtoms() == 0 || trade.GetQuoteQuantityAtoms() == 0 || trade.GetTradeId() == "" {
			return errors.New("postgres public: invalid trade")
		}
		payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(trade)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO market_data.public_trades
           (topic,source_market_id,market_id,market_sequence,event_ordinal,trade_id,recorded_at_ns,price_units,base_atoms,quote_atoms,payload)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			topic, int64(p.MarketID), trade.GetMarketId(), fmt.Sprint(trade.GetMatchSequence()), fmt.Sprint(trade.GetEventOrdinal()),
			trade.GetTradeId(), NanosDecimal(trade.GetRecordedAt().AsTime()), fmt.Sprint(trade.GetPriceUnits()),
			fmt.Sprint(trade.GetBaseQuantityAtoms()), fmt.Sprint(trade.GetQuoteQuantityAtoms()), payload); err != nil {
			return err
		}
		for _, interval := range statistics.SupportedIntervals() {
			start, _, err := statistics.Window(trade.GetRecordedAt().AsTime(), interval)
			if err != nil {
				return err
			}
			affected[intervalKey{interval: interval, start: start}] = struct{}{}
		}
	}
	// Sorting is unnecessary for SQL correctness but stabilizes transaction
	// lock acquisition order and makes failures reproducible in tests.
	keys := make([]intervalKey, 0, len(affected))
	for key := range affected {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].interval != keys[j].interval {
			return keys[i].interval < keys[j].interval
		}
		return keys[i].start.Before(keys[j].start)
	})
	for _, key := range keys {
		candle, err := s.rebuildCandle(ctx, tx, topic, p.MarketID, p.Snapshot.GetMarketId(), key.interval, key.start)
		if err != nil {
			return err
		}
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(candle)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO market_data.candle_updates
            (topic,source_market_id,market_sequence,interval_id,start_ns,payload)
            VALUES ($1,$2,$3,$4,$5,$6)`, topic, int64(p.MarketID),
			fmt.Sprint(p.Delta.GetMarketSequence()), int32(key.interval), NanosDecimal(key.start), encoded); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) rebuildCandle(ctx context.Context, tx *sql.Tx, topic string, marketID uint32, symbol string, interval md.CandleInterval, start time.Time) (*md.Candle, error) {
	_, end, err := statistics.Window(start, interval)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT payload FROM market_data.public_trades
        WHERE topic=$1 AND source_market_id=$2 AND recorded_at_ns >= $3 AND recorded_at_ns < $4
        ORDER BY market_sequence,event_ordinal`, topic, int64(marketID), NanosDecimal(start), NanosDecimal(end))
	if err != nil {
		return nil, err
	}
	var ticks []*md.TradeTick
	for rows.Next() {
		var payload []byte
		if err = rows.Scan(&payload); err != nil {
			break
		}
		tick := new(md.TradeTick)
		if err = proto.Unmarshal(payload, tick); err != nil {
			break
		}
		ticks = append(ticks, tick)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(ticks) == 0 {
		return nil, errors.New("postgres public: affected candle contains zero trades")
	}
	var current int64
	err = tx.QueryRowContext(ctx, `SELECT revision FROM market_data.candles
        WHERE topic=$1 AND source_market_id=$2 AND interval_id=$3 AND start_ns=$4 FOR UPDATE`,
		topic, int64(marketID), int32(interval), NanosDecimal(start)).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var revision uint64
	if err == nil {
		if current < 0 || current == math.MaxInt64 {
			return nil, errors.New("postgres public: candle revision overflow")
		}
		revision = uint64(current) + 1
	}
	candle, err := statistics.Candle(symbol, interval, start, ticks, revision)
	if err != nil {
		return nil, err
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(candle)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO market_data.candles
        (topic,source_market_id,market_id,interval_id,start_ns,end_ns,revision,payload)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (topic,source_market_id,interval_id,start_ns)
        DO UPDATE SET market_id=EXCLUDED.market_id,end_ns=EXCLUDED.end_ns,
            revision=EXCLUDED.revision,payload=EXCLUDED.payload`,
		topic, int64(marketID), symbol, int32(interval), NanosDecimal(start), NanosDecimal(end), int64(revision), payload); err != nil {
		return nil, err
	}
	return candle, nil
}
