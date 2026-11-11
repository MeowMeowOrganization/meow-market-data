// Package statistics derives deterministic OHLCV and rolling aggregates from
// canonical public trade ticks, never from Kafka arrival or Redis time.
package statistics

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrInvalid = errors.New("statistics: invalid trade data")

// In source event order, not timestamp order. This is stable during replay,
// including a late-recorded trade inside an older candle interval.
func sortTrades(ts []*md.TradeTick) {
	sort.Slice(ts, func(i, j int) bool {
		if ts[i].GetMatchSequence() != ts[j].GetMatchSequence() {
			return ts[i].GetMatchSequence() < ts[j].GetMatchSequence()
		}
		return ts[i].GetEventOrdinal() < ts[j].GetEventOrdinal()
	})
}

func intervalDuration(i md.CandleInterval) (time.Duration, error) {
	switch i {
	case md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE:
		return time.Minute, nil
	case md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES:
		return 5 * time.Minute, nil
	case md.CandleInterval_CANDLE_INTERVAL_FIFTEEN_MINUTES:
		return 15 * time.Minute, nil
	case md.CandleInterval_CANDLE_INTERVAL_ONE_HOUR:
		return time.Hour, nil
	case md.CandleInterval_CANDLE_INTERVAL_ONE_DAY:
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("%w: unsupported interval %d", ErrInvalid, i)
	}
}

func SupportedIntervals() []md.CandleInterval {
	return []md.CandleInterval{
		md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE,
		md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES,
		md.CandleInterval_CANDLE_INTERVAL_FIFTEEN_MINUTES,
		md.CandleInterval_CANDLE_INTERVAL_ONE_HOUR,
		md.CandleInterval_CANDLE_INTERVAL_ONE_DAY,
	}
}

func Window(t time.Time, i md.CandleInterval) (time.Time, time.Time, error) {
	d, err := intervalDuration(i)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start := t.UTC().Truncate(d)
	return start, start.Add(d), nil
}

type accumulation struct {
	open, high, low, close uint64
	base, quote            *big.Int
	count                  uint64
}

func (a *accumulation) add(t *md.TradeTick) error {
	if t == nil || t.GetMarketId() == "" || t.GetPriceUnits() == 0 || t.GetBaseQuantityAtoms() == 0 || t.GetQuoteQuantityAtoms() == 0 || t.GetRecordedAt() == nil || t.GetRecordedAt().CheckValid() != nil {
		return ErrInvalid
	}
	if a.count == ^uint64(0) {
		return fmt.Errorf("%w: trade count overflow", ErrInvalid)
	}
	price := t.GetPriceUnits()
	if a.count == 0 {
		a.open, a.high, a.low = price, price, price
		a.base = new(big.Int)
		a.quote = new(big.Int)
	}
	if price > a.high {
		a.high = price
	}
	if price < a.low {
		a.low = price
	}
	a.close = price
	a.base.Add(a.base, new(big.Int).SetUint64(t.GetBaseQuantityAtoms()))
	a.quote.Add(a.quote, new(big.Int).SetUint64(t.GetQuoteQuantityAtoms()))
	a.count++
	return nil
}

func aggregate(market string, rows []*md.TradeTick) (accumulation, error) {
	if market == "" || len(rows) == 0 {
		return accumulation{}, ErrInvalid
	}
	copyRows := append([]*md.TradeTick(nil), rows...)
	sortTrades(copyRows)
	var a accumulation
	var prevSeq, prevOrd uint64
	for i, t := range copyRows {
		if t == nil || t.GetMarketId() != market {
			return accumulation{}, ErrInvalid
		}
		if i > 0 && (t.GetMatchSequence() < prevSeq || (t.GetMatchSequence() == prevSeq && t.GetEventOrdinal() <= prevOrd)) {
			return accumulation{}, fmt.Errorf("%w: duplicate trade identity", ErrInvalid)
		}
		if err := a.add(t); err != nil {
			return accumulation{}, err
		}
		prevSeq, prevOrd = t.GetMatchSequence(), t.GetEventOrdinal()
	}
	return a, nil
}

func Candle(market string, interval md.CandleInterval, start time.Time, rows []*md.TradeTick, revision uint64) (*md.Candle, error) {
	begin, end, err := Window(start, interval)
	if err != nil {
		return nil, err
	}
	if !begin.Equal(start.UTC()) {
		return nil, fmt.Errorf("%w: unaligned candle start", ErrInvalid)
	}
	for _, t := range rows {
		if t == nil || t.GetRecordedAt() == nil {
			return nil, ErrInvalid
		}
		stamp := t.GetRecordedAt().AsTime()
		if stamp.Before(begin) || !stamp.Before(end) {
			return nil, fmt.Errorf("%w: trade outside candle", ErrInvalid)
		}
	}
	a, err := aggregate(market, rows)
	if err != nil {
		return nil, err
	}
	return &md.Candle{MarketId: market, Interval: interval, StartTime: timestamppb.New(begin), EndTime: timestamppb.New(end),
		OpenPriceUnits: a.open, HighPriceUnits: a.high, LowPriceUnits: a.low, ClosePriceUnits: a.close,
		BaseVolumeAtoms: a.base.String(), QuoteVolumeAtoms: a.quote.String(), TradeCount: a.count,
		Revision: revision, IsFinal: false, // no event-time watermark has been guaranteed
	}, nil
}

// Stats24h always returns valid zero-volume statistics for empty windows.
// The window is (asOf - 24h, asOf], not the UTC calendar day.
func Stats24h(market string, asOf time.Time, rows []*md.TradeTick, reference ...uint64) (*md.Stats24H, error) {
	if market == "" {
		return nil, ErrInvalid
	}
	asOf = asOf.UTC()
	start := asOf.Add(-24 * time.Hour)
	out := &md.Stats24H{MarketId: market, WindowStartExclusive: timestamppb.New(start), AsOfInclusive: timestamppb.New(asOf), BaseVolumeAtoms: "0", QuoteVolumeAtoms: "0", PriceChangeBps: ""}
	if len(rows) == 0 {
		return out, nil
	}
	for _, t := range rows {
		if t == nil || t.GetRecordedAt() == nil {
			return nil, ErrInvalid
		}
		stamp := t.GetRecordedAt().AsTime()
		if !stamp.After(start) || stamp.After(asOf) {
			return nil, fmt.Errorf("%w: out-of-window trade", ErrInvalid)
		}
	}
	a, err := aggregate(market, rows)
	if err != nil {
		return nil, err
	}
	open, high, low, close := a.open, a.high, a.low, a.close
	out.OpenPriceUnits, out.HighPriceUnits, out.LowPriceUnits, out.ClosePriceUnits = &open, &high, &low, &close
	out.BaseVolumeAtoms, out.QuoteVolumeAtoms, out.TradeCount = a.base.String(), a.quote.String(), a.count
	if len(reference) > 0 {
		if len(reference) != 1 || reference[0] == 0 {
			return nil, ErrInvalid
		}
		diff := new(big.Int).Sub(new(big.Int).SetUint64(close), new(big.Int).SetUint64(reference[0]))
		diff.Mul(diff, big.NewInt(10000))
		diff.Quo(diff, new(big.Int).SetUint64(reference[0]))
		out.PriceChangeBps = diff.String()
	}
	return out, nil
}
