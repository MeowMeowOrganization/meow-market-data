package statistics

import (
	"fmt"
	"math"
	"math/big"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MinuteRollup is the exact, durable summary of one non-empty UTC minute.
// First/last refer to matching (sequence, event ordinal), NOT recorded time.
// Volumes are decimal integers because their sum may exceed uint64.
// The database primary key is (topic, source_market_id, minute_start_ns).
type MinuteRollup struct {
	Start            time.Time
	MarketID         string
	FirstSequence    uint64
	FirstOrdinal     uint64
	FirstPriceUnits  uint64
	LastSequence     uint64
	LastOrdinal      uint64
	LastPriceUnits   uint64
	HighPriceUnits   uint64
	LowPriceUnits    uint64
	BaseVolumeAtoms  string
	QuoteVolumeAtoms string
	TradeCount       uint64
}

func sourceBefore(aSeq, aOrd, bSeq, bOrd uint64) bool {
	return aSeq < bSeq || (aSeq == bSeq && aOrd < bOrd)
}

func positiveDecimal(v string) (*big.Int, error) {
	if v == "" {
		return nil, ErrInvalid
	}
	for _, c := range v {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("%w: non-decimal volume", ErrInvalid)
		}
	}
	n, ok := new(big.Int).SetString(v, 10)
	if !ok || n.Sign() <= 0 {
		return nil, fmt.Errorf("%w: non-positive volume", ErrInvalid)
	}
	return n, nil
}

// CandleFromMinuteRollups materializes the same exact OHLCV as Candle, but
// touches at most 1/5/15/60/1440 minute rows regardless of trade frequency.
// It is safe for late-recorded trades: each revised minute rollup carries the
// earliest/latest matching identities and the candle revision is incremented
// by the caller in its PostgreSQL transaction.
func CandleFromMinuteRollups(market string, interval md.CandleInterval, start time.Time, rows []MinuteRollup, revision uint64) (*md.Candle, error) {
	begin, end, err := Window(start, interval)
	if err != nil {
		return nil, err
	}
	if market == "" || !begin.Equal(start.UTC()) || len(rows) == 0 || len(rows) > int(end.Sub(begin)/time.Minute) {
		return nil, fmt.Errorf("%w: missing, excessive or unaligned minute rollups", ErrInvalid)
	}

	var firstSeq, firstOrd, lastSeq, lastOrd uint64
	var open, high, low, close, count uint64
	var base, quote big.Int
	seen := make(map[time.Time]struct{}, len(rows))
	for _, r := range rows {
		minute := r.Start.UTC()
		if r.MarketID != market || !minute.Equal(minute.Truncate(time.Minute)) || minute.Before(begin) || !minute.Before(end) {
			return nil, fmt.Errorf("%w: minute does not belong to candle", ErrInvalid)
		}
		if _, exists := seen[minute]; exists {
			return nil, fmt.Errorf("%w: duplicated minute", ErrInvalid)
		}
		seen[minute] = struct{}{}
		if r.FirstSequence == 0 || r.LastSequence == 0 || r.TradeCount == 0 ||
			r.FirstPriceUnits == 0 || r.LastPriceUnits == 0 || r.LowPriceUnits == 0 ||
			r.HighPriceUnits < r.LowPriceUnits ||
			r.FirstPriceUnits < r.LowPriceUnits || r.FirstPriceUnits > r.HighPriceUnits ||
			r.LastPriceUnits < r.LowPriceUnits || r.LastPriceUnits > r.HighPriceUnits ||
			sourceBefore(r.LastSequence, r.LastOrdinal, r.FirstSequence, r.FirstOrdinal) {
			return nil, fmt.Errorf("%w: inconsistent minute rollup", ErrInvalid)
		}
		b, err := positiveDecimal(r.BaseVolumeAtoms)
		if err != nil {
			return nil, err
		}
		q, err := positiveDecimal(r.QuoteVolumeAtoms)
		if err != nil {
			return nil, err
		}
		if count > math.MaxUint64-r.TradeCount {
			return nil, fmt.Errorf("%w: trade count overflow", ErrInvalid)
		}
		if count == 0 || sourceBefore(r.FirstSequence, r.FirstOrdinal, firstSeq, firstOrd) {
			firstSeq, firstOrd, open = r.FirstSequence, r.FirstOrdinal, r.FirstPriceUnits
		}
		if count == 0 || sourceBefore(lastSeq, lastOrd, r.LastSequence, r.LastOrdinal) {
			lastSeq, lastOrd, close = r.LastSequence, r.LastOrdinal, r.LastPriceUnits
		}
		if count == 0 || high < r.HighPriceUnits {
			high = r.HighPriceUnits
		}
		if count == 0 || low > r.LowPriceUnits {
			low = r.LowPriceUnits
		}
		base.Add(&base, b)
		quote.Add(&quote, q)
		count += r.TradeCount
	}
	return &md.Candle{
		MarketId: market, Interval: interval,
		StartTime: timestamppb.New(begin), EndTime: timestamppb.New(end),
		OpenPriceUnits: open, HighPriceUnits: high, LowPriceUnits: low, ClosePriceUnits: close,
		BaseVolumeAtoms: base.String(), QuoteVolumeAtoms: quote.String(),
		TradeCount: count, Revision: revision, IsFinal: false,
	}, nil
}
