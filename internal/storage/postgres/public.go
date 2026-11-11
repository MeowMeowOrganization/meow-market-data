package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matching "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	"github.com/MeowMeowOrganization/meow-contracts/tradeid"
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
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&envelope)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	if p.Delta == nil {
		if len(p.Trades) != 0 {
			return errors.New("postgres public: duplicate cannot contain trades")
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO market_data.matching_result_provenance (topic,source_market_id,market_sequence,payload_sha256) VALUES($1,$2,$3,$4)`, topic, int64(p.MarketID), fmt.Sprint(envelope.GetResult().GetMarketSequence()), digest[:]); err != nil {
		return fmt.Errorf("postgres: conflicting or duplicate source provenance: %w", err)
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
	// affected UTC intervals once, then materialize FROM durable minute rollups
	// after all executions and their minute UPSERTs are complete.
	type intervalKey struct {
		interval md.CandleInterval
		start    time.Time
	}
	affected := make(map[intervalKey]struct{})
	type minuteTotal struct {
		symbol                                                      string
		firstSeq, firstOrd, firstPrice, lastSeq, lastOrd, lastPrice uint64
		high, low                                                   uint64
		base, quote                                                 big.Int
		count                                                       uint64
	}
	minuteTotals := make(map[time.Time]*minuteTotal)
	for _, trade := range p.Trades {
		if trade == nil || trade.GetSourceMarketId() != p.MarketID || trade.GetMarketId() != p.Snapshot.GetMarketId() ||
			trade.GetMatchSequence() != p.Delta.GetMarketSequence() || trade.GetRecordedAt() == nil || trade.GetRecordedAt().CheckValid() != nil ||
			trade.GetPriceUnits() == 0 || trade.GetBaseQuantityAtoms() == 0 || trade.GetQuoteQuantityAtoms() == 0 || trade.GetTradeId() == "" {
			return errors.New("postgres public: invalid trade")
		}
		identityMarket, identitySeq, identityOrd, identityErr := tradeid.Parse(trade.GetTradeId())
		if identityErr != nil || identityMarket != p.MarketID || identitySeq != trade.GetMatchSequence() || identityOrd != trade.GetEventOrdinal() {
			return errors.New("postgres public: noncanonical trade identity")
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
		// Accumulate in memory first. One SQL upsert per affected minute,
		// rather than a hot-row update for every fill in the matching result.
		minuteStart := trade.GetRecordedAt().AsTime().UTC().Truncate(time.Minute)
		minute := minuteTotals[minuteStart]
		if minute == nil {
			minute = &minuteTotal{symbol: trade.GetMarketId(), firstSeq: trade.GetMatchSequence(), firstOrd: trade.GetEventOrdinal(),
				firstPrice: trade.GetPriceUnits(), lastSeq: trade.GetMatchSequence(), lastOrd: trade.GetEventOrdinal(),
				lastPrice: trade.GetPriceUnits(), high: trade.GetPriceUnits(), low: trade.GetPriceUnits()}
			minuteTotals[minuteStart] = minute
		}
		if minute.symbol != trade.GetMarketId() || minute.count == math.MaxUint64 {
			return errors.New("postgres public: minute aggregate mismatch or count overflow")
		}
		if trade.GetMatchSequence() < minute.firstSeq || (trade.GetMatchSequence() == minute.firstSeq && trade.GetEventOrdinal() < minute.firstOrd) {
			minute.firstSeq, minute.firstOrd, minute.firstPrice = trade.GetMatchSequence(), trade.GetEventOrdinal(), trade.GetPriceUnits()
		}
		if trade.GetMatchSequence() > minute.lastSeq || (trade.GetMatchSequence() == minute.lastSeq && trade.GetEventOrdinal() > minute.lastOrd) {
			minute.lastSeq, minute.lastOrd, minute.lastPrice = trade.GetMatchSequence(), trade.GetEventOrdinal(), trade.GetPriceUnits()
		}
		if trade.GetPriceUnits() > minute.high {
			minute.high = trade.GetPriceUnits()
		}
		if trade.GetPriceUnits() < minute.low {
			minute.low = trade.GetPriceUnits()
		}
		minute.base.Add(&minute.base, new(big.Int).SetUint64(trade.GetBaseQuantityAtoms()))
		minute.quote.Add(&minute.quote, new(big.Int).SetUint64(trade.GetQuoteQuantityAtoms()))
		minute.count++
		for _, interval := range statistics.SupportedIntervals() {
			start, _, err := statistics.Window(trade.GetRecordedAt().AsTime(), interval)
			if err != nil {
				return err
			}
			affected[intervalKey{interval: interval, start: start}] = struct{}{}
		}
	}
	// Persist exact per-minute aggregates atomically with the trades. Sorting
	// also keeps lock acquisition order deterministic across writers.
	minutes := make([]time.Time, 0, len(minuteTotals))
	for start := range minuteTotals {
		minutes = append(minutes, start)
	}
	sort.Slice(minutes, func(i, j int) bool { return minutes[i].Before(minutes[j]) })
	for _, start := range minutes {
		r := minuteTotals[start]
		var updatedSymbol string
		if err := tx.QueryRowContext(ctx, `INSERT INTO market_data.trade_minute_rollups
          (topic,source_market_id,minute_start_ns,market_id,
           first_sequence,first_ordinal,first_price_units,
           last_sequence,last_ordinal,last_price_units,
           high_price_units,low_price_units,base_volume_atoms,quote_volume_atoms,trade_count)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
          ON CONFLICT (topic,source_market_id,minute_start_ns) DO UPDATE SET
            first_sequence = CASE WHEN (EXCLUDED.first_sequence,EXCLUDED.first_ordinal) <
                (trade_minute_rollups.first_sequence,trade_minute_rollups.first_ordinal)
                THEN EXCLUDED.first_sequence ELSE trade_minute_rollups.first_sequence END,
            first_ordinal = CASE WHEN (EXCLUDED.first_sequence,EXCLUDED.first_ordinal) <
                (trade_minute_rollups.first_sequence,trade_minute_rollups.first_ordinal)
                THEN EXCLUDED.first_ordinal ELSE trade_minute_rollups.first_ordinal END,
            first_price_units = CASE WHEN (EXCLUDED.first_sequence,EXCLUDED.first_ordinal) <
                (trade_minute_rollups.first_sequence,trade_minute_rollups.first_ordinal)
                THEN EXCLUDED.first_price_units ELSE trade_minute_rollups.first_price_units END,
            last_sequence = CASE WHEN (EXCLUDED.last_sequence,EXCLUDED.last_ordinal) >
                (trade_minute_rollups.last_sequence,trade_minute_rollups.last_ordinal)
                THEN EXCLUDED.last_sequence ELSE trade_minute_rollups.last_sequence END,
            last_ordinal = CASE WHEN (EXCLUDED.last_sequence,EXCLUDED.last_ordinal) >
                (trade_minute_rollups.last_sequence,trade_minute_rollups.last_ordinal)
                THEN EXCLUDED.last_ordinal ELSE trade_minute_rollups.last_ordinal END,
            last_price_units = CASE WHEN (EXCLUDED.last_sequence,EXCLUDED.last_ordinal) >
                (trade_minute_rollups.last_sequence,trade_minute_rollups.last_ordinal)
                THEN EXCLUDED.last_price_units ELSE trade_minute_rollups.last_price_units END,
            high_price_units = GREATEST(trade_minute_rollups.high_price_units,EXCLUDED.high_price_units),
            low_price_units = LEAST(trade_minute_rollups.low_price_units,EXCLUDED.low_price_units),
            base_volume_atoms = trade_minute_rollups.base_volume_atoms + EXCLUDED.base_volume_atoms,
            quote_volume_atoms = trade_minute_rollups.quote_volume_atoms + EXCLUDED.quote_volume_atoms,
            trade_count = trade_minute_rollups.trade_count + EXCLUDED.trade_count
          WHERE trade_minute_rollups.market_id = EXCLUDED.market_id
          RETURNING market_id`,
			topic, int64(p.MarketID), NanosDecimal(start), r.symbol,
			fmt.Sprint(r.firstSeq), fmt.Sprint(r.firstOrd), fmt.Sprint(r.firstPrice),
			fmt.Sprint(r.lastSeq), fmt.Sprint(r.lastOrd), fmt.Sprint(r.lastPrice),
			fmt.Sprint(r.high), fmt.Sprint(r.low), r.base.String(), r.quote.String(), fmt.Sprint(r.count)).Scan(&updatedSymbol); err != nil {
			return fmt.Errorf("postgres public: minute rollup: %w", err)
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
	// A no-fill result still advances event time, allowing prior candles to
	// become final even while trading is quiet. This stays in the Kafka TX.
	if err := advanceWatermark(ctx, tx, topic, p.MarketID, envelope.GetRecordedAt().AsTime(), p.Snapshot.GetMarketSequence()); err != nil {
		return err
	}
	return nil
}

// rebuildCandle reads only exact persisted minute summaries. Even a one-day
// candle now requires at most 1440 rows (never every trade in that day).
// The call is inside the same fenced Kafka-offset transaction as the trades,
// rollup UPSERTs, candle UPDATE and durable candle_updates journal.
func (s *Store) rebuildCandle(ctx context.Context, tx *sql.Tx, topic string, marketID uint32, symbol string, interval md.CandleInterval, start time.Time) (*md.Candle, error) {
	_, end, err := statistics.Window(start, interval)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT minute_start_ns::text,market_id,
        first_sequence::text,first_ordinal::text,first_price_units::text,
        last_sequence::text,last_ordinal::text,last_price_units::text,
        high_price_units::text,low_price_units::text,
        base_volume_atoms::text,quote_volume_atoms::text,trade_count::text
        FROM market_data.trade_minute_rollups
        WHERE topic=$1 AND source_market_id=$2 AND minute_start_ns >= $3 AND minute_start_ns < $4
        ORDER BY minute_start_ns`, topic, int64(marketID), NanosDecimal(start), NanosDecimal(end))
	if err != nil {
		return nil, err
	}
	rollups := make([]statistics.MinuteRollup, 0, int(end.Sub(start)/time.Minute))
	for rows.Next() {
		var r statistics.MinuteRollup
		var ns string
		if err = rows.Scan(&ns, &r.MarketID,
			&r.FirstSequence, &r.FirstOrdinal, &r.FirstPriceUnits,
			&r.LastSequence, &r.LastOrdinal, &r.LastPriceUnits,
			&r.HighPriceUnits, &r.LowPriceUnits,
			&r.BaseVolumeAtoms, &r.QuoteVolumeAtoms, &r.TradeCount); err != nil {
			break
		}
		r.Start, err = parseNanosDecimal(ns)
		if err != nil {
			break
		}
		rollups = append(rollups, r)
		if len(rollups) > int(end.Sub(start)/time.Minute) {
			err = errors.New("postgres public: too many minute rollups for interval")
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close() // close before next query on this transaction
	if err != nil {
		return nil, fmt.Errorf("postgres public: read minute rollups: %w", err)
	}
	if len(rollups) == 0 {
		return nil, errors.New("postgres public: affected candle contains zero minute rollups")
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
	candle, err := statistics.CandleFromMinuteRollups(symbol, interval, start, rollups, revision)
	if err != nil {
		return nil, err
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(candle)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO market_data.candles
        (topic,source_market_id,market_id,interval_id,start_ns,end_ns,revision,payload,is_final)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,FALSE)
        ON CONFLICT (topic,source_market_id,interval_id,start_ns)
        DO UPDATE SET market_id=EXCLUDED.market_id,end_ns=EXCLUDED.end_ns,
            revision=EXCLUDED.revision,payload=EXCLUDED.payload,is_final=FALSE`,
		topic, int64(marketID), symbol, int32(interval), NanosDecimal(start), NanosDecimal(end), int64(revision), payload); err != nil {
		return nil, err
	}
	return candle, nil
}

// parseNanosDecimal converts a PostgreSQL NUMERIC(30,0) timestamp without
// int64 nanosecond overflow or microsecond timestamp rounding. Negative Unix
// timestamps are handled by time.Unix normalization.
func parseNanosDecimal(raw string) (time.Time, error) {
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		return time.Time{}, errors.New("postgres public: invalid minute start nanoseconds")
	}
	sec, ns := new(big.Int).QuoRem(n, big.NewInt(1_000_000_000), new(big.Int))
	if !sec.IsInt64() {
		return time.Time{}, errors.New("postgres public: minute timestamp outside range")
	}
	stamp := time.Unix(sec.Int64(), ns.Int64()).UTC()
	if NanosDecimal(stamp) != raw {
		return time.Time{}, errors.New("postgres public: minute timestamp not canonical")
	}
	return stamp, nil
}
