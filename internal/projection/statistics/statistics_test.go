package statistics

import (
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func tick(seq, ord uint64, price, base, quote uint64, at time.Time) *md.TradeTick {
	return &md.TradeTick{MarketId: "BTC-USDT", MatchSequence: seq, EventOrdinal: ord, PriceUnits: price, BaseQuantityAtoms: base, QuoteQuantityAtoms: quote, RecordedAt: timestamppb.New(at)}
}
func TestCandleUsesSourceOrderNotRecordedTime(t *testing.T) {
	base := time.Date(2026, 11, 10, 12, 00, 0, 0, time.UTC)
	a := tick(1, 0, 100, 7, 10, base.Add(59*time.Second))
	b := tick(2, 0, 110, 3, 20, base.Add(time.Second)) // later source, earlier event time
	candle, err := Candle("BTC-USDT", md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, base, []*md.TradeTick{b, a}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if candle.GetOpenPriceUnits() != 100 || candle.GetClosePriceUnits() != 110 || candle.GetHighPriceUnits() != 110 || candle.GetLowPriceUnits() != 100 || candle.GetBaseVolumeAtoms() != "10" || candle.GetQuoteVolumeAtoms() != "30" || candle.GetRevision() != 2 || candle.GetIsFinal() {
		t.Fatalf("candle: %+v", candle)
	}
}
func TestRollingWindowBoundsAndSignedBPS(t *testing.T) {
	asOf := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	start := asOf.Add(-24 * time.Hour)
	_, err := Stats24h("BTC-USDT", asOf, []*md.TradeTick{tick(1, 0, 200, 1, 1, start)})
	if err == nil {
		t.Fatal("exclusive window start accepted")
	}
	rows := []*md.TradeTick{tick(1, 0, 200, 3, 9, start.Add(time.Nanosecond)), tick(2, 0, 150, 4, 11, asOf)}
	s, err := Stats24h("BTC-USDT", asOf, rows, 200)
	if err != nil {
		t.Fatal(err)
	}
	if s.GetOpenPriceUnits() != 200 || s.GetClosePriceUnits() != 150 || s.GetPriceChangeBps() != "-2500" || s.GetBaseVolumeAtoms() != "7" || s.GetTradeCount() != 2 {
		t.Fatalf("stats: %+v", s)
	}
	empty, err := Stats24h("BTC-USDT", asOf, nil)
	if err != nil || empty.GetTradeCount() != 0 || empty.GetBaseVolumeAtoms() != "0" {
		t.Fatalf("empty: %+v %v", empty, err)
	}
}
func TestCandleIntervalBoundary(t *testing.T) {
	start := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	_, err := Candle("BTC-USDT", md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, start, []*md.TradeTick{tick(1, 0, 10, 1, 1, start.Add(time.Minute))}, 0)
	if err == nil {
		t.Fatal("included exclusive end")
	}
}

func TestPercentageChangeUsesStrictHistoricalReference(t *testing.T) {
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	ticks := []*md.TradeTick{tick(1, 0, 150, 2, 3, now.Add(-time.Hour)), tick(2, 0, 200, 2, 3, now)}
	noRef, e := Stats24h("BTC-USDT", now, ticks)
	if e != nil || noRef.GetPriceChangeBps() != "" {
		t.Fatalf("fabricated missing reference: %v %v", noRef, e)
	}
	ref, e := Stats24h("BTC-USDT", now, ticks, 100)
	if e != nil || ref.GetOpenPriceUnits() != 150 || ref.GetClosePriceUnits() != 200 || ref.GetPriceChangeBps() != "10000" {
		t.Fatalf("wrong cutoff reference: %v %v", ref, e)
	}
}
