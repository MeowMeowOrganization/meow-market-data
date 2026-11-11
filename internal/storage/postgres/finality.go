package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// MaximumEventLateness defines the contract with the matching publisher:
// recorded_at may move backward within this bound relative to the highest
// accepted source event-time. Events outside the bound fail closed. Only
// source-event advancement (never wall clock) moves the watermark.
const MaximumEventLateness = 48 * time.Hour

func advanceWatermark(ctx context.Context, tx *sql.Tx, topic string, id uint32, at time.Time, sequence uint64) error {
	var previous string
	err := tx.QueryRowContext(ctx, `SELECT max_recorded_at_ns::text FROM market_data.market_event_watermarks WHERE topic=$1 AND source_market_id=$2 FOR UPDATE`, topic, int64(id)).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	maxTime := at.UTC()
	if err == nil {
		last, e := parseNanosDecimal(previous)
		if e != nil {
			return e
		}
		if at.Before(last.Add(-MaximumEventLateness)) {
			return fmt.Errorf("postgres: event-time lateness exceeds %s: market=%d sequence=%d", MaximumEventLateness, id, sequence)
		}
		if last.After(maxTime) {
			maxTime = last
		}
	}
	if _, e := tx.ExecContext(ctx, `INSERT INTO market_data.market_event_watermarks(topic,source_market_id,max_recorded_at_ns) VALUES($1,$2,$3) ON CONFLICT(topic,source_market_id) DO UPDATE SET max_recorded_at_ns=EXCLUDED.max_recorded_at_ns`, topic, int64(id), NanosDecimal(maxTime)); e != nil {
		return e
	}
	// Finalize only after the *entire* event has been materialized. A candle is
	// final once its exclusive end <= source high-watermark minus 48 hours.
	// Cap per-record work at 256 candidates; remaining eligible candles are
	// finalized on subsequent committed source results without blocking Kafka.
	watermark := maxTime.Add(-MaximumEventLateness)
	rows, e := tx.QueryContext(ctx, `SELECT interval_id,start_ns::text,payload,revision FROM market_data.candles
    WHERE topic=$1 AND source_market_id=$2 AND end_ns <= $3 AND NOT is_final
    ORDER BY end_ns,interval_id,start_ns LIMIT 256 FOR UPDATE`, topic, int64(id), NanosDecimal(watermark))
	if e != nil {
		return e
	}
	type candidate struct {
		interval int32
		start    string
		raw      []byte
		revision int64
	}
	var candidates []candidate
	for rows.Next() {
		var v candidate
		if e = rows.Scan(&v.interval, &v.start, &v.raw, &v.revision); e != nil {
			break
		}
		candidates = append(candidates, v)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	for _, v := range candidates {
		candle := new(md.Candle)
		if e = proto.Unmarshal(v.raw, candle); e != nil {
			return e
		}
		if candle.GetIsFinal() {
			continue
		}
		if v.revision < 0 || v.revision == math.MaxInt64 || uint64(v.revision) != candle.GetRevision() {
			return errors.New("postgres: corrupt candle revision")
		}
		candle.IsFinal = true
		candle.Revision++
		encoded, e := (proto.MarshalOptions{Deterministic: true}).Marshal(candle)
		if e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, `UPDATE market_data.candles SET revision=$5,payload=$6,is_final=TRUE
      WHERE topic=$1 AND source_market_id=$2 AND interval_id=$3 AND start_ns=$4`, topic, int64(id), v.interval, v.start, v.revision+1, encoded); e != nil {
			return e
		}
		// A fresh candle correction and its finalization may occur at the same
		// sequence. The journal key has one final version per key and sequence.
		if _, e = tx.ExecContext(ctx, `INSERT INTO market_data.candle_updates(topic,source_market_id,market_sequence,interval_id,start_ns,payload)
      VALUES($1,$2,$3,$4,$5,$6)
      ON CONFLICT(topic,source_market_id,market_sequence,interval_id,start_ns) DO UPDATE SET payload=EXCLUDED.payload`, topic, int64(id), fmt.Sprint(sequence), v.interval, v.start, encoded); e != nil {
			return e
		}
	}
	return nil
}
