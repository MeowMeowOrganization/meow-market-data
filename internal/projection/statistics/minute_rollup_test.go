package statistics

import (
	"math"
	"math/big"
	"math/rand"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// Build rollups independently from source-order sorted trade ticks. This is
// intentionally a test-only oracle, not the code used by the PG projector.
func rollupsFromTrades(t *testing.T, trades []*md.TradeTick) []MinuteRollup {
	t.Helper()
	buckets := make(map[time.Time][]*md.TradeTick)
	for _, trade := range trades {
		minute := trade.GetRecordedAt().AsTime().UTC().Truncate(time.Minute)
		buckets[minute] = append(buckets[minute], trade)
	}
	out := make([]MinuteRollup, 0, len(buckets))
	for start, rows := range buckets {
		sortTrades(rows)
		var base, quote big.Int
		r := MinuteRollup{
			Start: start, MarketID: rows[0].GetMarketId(),
			FirstSequence: rows[0].GetMatchSequence(), FirstOrdinal: rows[0].GetEventOrdinal(),
			FirstPriceUnits: rows[0].GetPriceUnits(),
			LastSequence:    rows[len(rows)-1].GetMatchSequence(), LastOrdinal: rows[len(rows)-1].GetEventOrdinal(),
			LastPriceUnits: rows[len(rows)-1].GetPriceUnits(), HighPriceUnits: rows[0].GetPriceUnits(),
			LowPriceUnits: rows[0].GetPriceUnits(), TradeCount: uint64(len(rows)),
		}
		for _, v := range rows {
			if v.GetPriceUnits() > r.HighPriceUnits {
				r.HighPriceUnits = v.GetPriceUnits()
			}
			if v.GetPriceUnits() < r.LowPriceUnits {
				r.LowPriceUnits = v.GetPriceUnits()
			}
			base.Add(&base, new(big.Int).SetUint64(v.GetBaseQuantityAtoms()))
			quote.Add(&quote, new(big.Int).SetUint64(v.GetQuoteQuantityAtoms()))
		}
		r.BaseVolumeAtoms, r.QuoteVolumeAtoms = base.String(), quote.String()
		out = append(out, r)
	}
	return out
}

func TestMinuteRollupDifferentialAllIntervalsLateRecordedExecutions(t *testing.T) {
	start := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	rng := rand.New(rand.NewSource(25))
	var trades []*md.TradeTick
	for seq := uint64(1); seq <= 1900; seq++ {
		// Source sequence deliberately differs from event-time order. Multiple
		// executions also share a sequence but have distinct event ordinals.
		for ord := uint64(0); ord < uint64(1+rng.Intn(3)); ord++ {
			at := start.Add(time.Duration(rng.Intn(24*60))*time.Minute + time.Duration(rng.Intn(60))*time.Second + time.Duration(rng.Intn(1000))*time.Nanosecond)
			trades = append(trades, tick(seq, ord, uint64(1+rng.Intn(100000)), uint64(1+rng.Intn(100000)), uint64(1+rng.Intn(100000)), at))
		}
	}
	rollups := rollupsFromTrades(t, trades)
	for _, interval := range SupportedIntervals() {
		seen := make(map[time.Time]struct{})
		for _, r := range rollups {
			begin, _, err := Window(r.Start, interval)
			if err != nil {
				t.Fatal(err)
			}
			seen[begin] = struct{}{}
		}
		for begin := range seen {
			_, end, _ := Window(begin, interval)
			var subset []*md.TradeTick
			var summaries []MinuteRollup
			for _, tr := range trades {
				at := tr.GetRecordedAt().AsTime()
				if !at.Before(begin) && at.Before(end) {
					subset = append(subset, tr)
				}
			}
			for _, r := range rollups {
				if !r.Start.Before(begin) && r.Start.Before(end) {
					summaries = append(summaries, r)
				}
			}
			expected, err := Candle("BTC-USDT", interval, begin, subset, 7)
			if err != nil {
				t.Fatal(err)
			}
			got, err := CandleFromMinuteRollups("BTC-USDT", interval, begin, summaries, 7)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(expected, got) {
				t.Fatalf("interval=%s begin=%s: rollups=%v full scan=%v", interval, begin, got, expected)
			}
		}
	}
}

func TestMinuteRollupSourceOrderAcrossMinutesAndHighVolume(t *testing.T) {
	start := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	r1 := MinuteRollup{
		Start: start.Add(time.Minute), MarketID: "BTC-USDT", FirstSequence: 1, LastSequence: 1,
		FirstPriceUnits: 50, LastPriceUnits: 50, HighPriceUnits: 50, LowPriceUnits: 50,
		BaseVolumeAtoms: "18446744073709551615", QuoteVolumeAtoms: "18446744073709551615", TradeCount: 1,
	}
	r2 := MinuteRollup{
		Start: start, MarketID: "BTC-USDT", FirstSequence: 2, LastSequence: 2,
		FirstPriceUnits: 60, LastPriceUnits: 60, HighPriceUnits: 60, LowPriceUnits: 60,
		BaseVolumeAtoms: "2", QuoteVolumeAtoms: "4", TradeCount: 1,
	}
	got, err := CandleFromMinuteRollups("BTC-USDT", md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES, start, []MinuteRollup{r2, r1}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetOpenPriceUnits() != 50 || got.GetClosePriceUnits() != 60 || got.GetBaseVolumeAtoms() != "18446744073709551617" || got.GetQuoteVolumeAtoms() != "18446744073709551619" || got.GetTradeCount() != 2 || got.GetRevision() != 3 || got.GetIsFinal() {
		t.Fatalf("source-order or exact-volume regression: %v", got)
	}
}

func TestMinuteRollupRejectsMalformedOrInconsistentState(t *testing.T) {
	start := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	r := MinuteRollup{Start: start, MarketID: "BTC-USDT", FirstSequence: 2, LastSequence: 2,
		FirstPriceUnits: 50, LastPriceUnits: 50, LowPriceUnits: 50, HighPriceUnits: 50,
		BaseVolumeAtoms: "1", QuoteVolumeAtoms: "2", TradeCount: 1}
	valid := func(rows ...MinuteRollup) error {
		_, err := CandleFromMinuteRollups("BTC-USDT", md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, start, rows, 1)
		return err
	}
	if err := valid(r); err != nil {
		t.Fatal(err)
	}
	bad := []MinuteRollup{
		func() MinuteRollup { b := r; b.Start = start.Add(time.Minute); return b }(),
		func() MinuteRollup { b := r; b.MarketID = "ETH-USDT"; return b }(),
		func() MinuteRollup { b := r; b.LowPriceUnits = 51; return b }(),
		func() MinuteRollup { b := r; b.FirstSequence = 3; return b }(),
		func() MinuteRollup { b := r; b.TradeCount = 0; return b }(),
		func() MinuteRollup { b := r; b.BaseVolumeAtoms = "-10"; return b }(),
		func() MinuteRollup { b := r; b.QuoteVolumeAtoms = "1.5"; return b }(),
		func() MinuteRollup { b := r; b.Start = start.Add(time.Nanosecond); return b }(),
	}
	for i, value := range bad {
		if err := valid(value); err == nil {
			t.Fatalf("accepted invalid rollup case %d", i)
		}
	}
	if err := valid(r, r); err == nil {
		t.Fatal("accepted duplicate/excessive 1-minute rollups")
	}
	if err := valid(); err == nil {
		t.Fatal("accepted missing rollups")
	}
	r.TradeCount = math.MaxUint64
	if _, err := CandleFromMinuteRollups("BTC-USDT", md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES, start, []MinuteRollup{r, {
		Start: start.Add(time.Minute), MarketID: "BTC-USDT", FirstSequence: 3, LastSequence: 3,
		FirstPriceUnits: 50, LastPriceUnits: 50, LowPriceUnits: 50, HighPriceUnits: 50,
		BaseVolumeAtoms: "1", QuoteVolumeAtoms: "1", TradeCount: 1,
	}}, 0); err == nil {
		t.Fatal("accepted trade count overflow")
	}
}

func BenchmarkCandleFromMinuteRollupsDay(b *testing.B) {
	start := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	rollups := make([]MinuteRollup, 1440)
	for i := range rollups {
		rollups[i] = MinuteRollup{
			Start: start.Add(time.Duration(i) * time.Minute), MarketID: "BTC-USDT",
			FirstSequence: uint64(i + 1), LastSequence: uint64(i + 1),
			FirstPriceUnits: 100, LastPriceUnits: 100, HighPriceUnits: 100, LowPriceUnits: 100,
			BaseVolumeAtoms: "1000000000000000000000", QuoteVolumeAtoms: "2500000000000000000000", TradeCount: 100000,
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := CandleFromMinuteRollups("BTC-USDT", md.CandleInterval_CANDLE_INTERVAL_ONE_DAY, start, rollups, 1); err != nil {
			b.Fatal(err)
		}
	}
}
