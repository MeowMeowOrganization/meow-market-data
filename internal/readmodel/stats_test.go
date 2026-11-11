package readmodel

import (
	"math"
	"math/big"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/statistics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStatsRowsRespectSourceSequenceNotMinuteTime(t *testing.T) {
	// Later matching sequence may have an earlier recorded timestamp.
	// The result must match the canonical trade-based algorithm exactly.
	now := time.Date(2026, 11, 11, 12, 34, 30, 0, time.UTC)
	trades := []*md.TradeTick{
		{MarketId: "BTC-USDT", MatchSequence: 10, EventOrdinal: 0, PriceUnits: 10000, BaseQuantityAtoms: 1000, QuoteQuantityAtoms: 2000, RecordedAt: timestamppb.New(now.Add(-time.Hour))},
		{MarketId: "BTC-USDT", MatchSequence: 11, EventOrdinal: 0, PriceUnits: 11000, BaseQuantityAtoms: 2000, QuoteQuantityAtoms: 4000, RecordedAt: timestamppb.New(now.Add(-2 * time.Hour))},
		{MarketId: "BTC-USDT", MatchSequence: 12, EventOrdinal: 0, PriceUnits: 9000, BaseQuantityAtoms: 3000, QuoteQuantityAtoms: 6000, RecordedAt: timestamppb.New(now.Add(-time.Minute))},
	}
	a := statsRow{firstSeq: 12, lastSeq: 12, firstPrice: 9000, lastPrice: 9000, high: 9000, low: 9000, count: 1, base: big.NewInt(3000), quote: big.NewInt(6000)}
	b := statsRow{firstSeq: 11, lastSeq: 11, firstPrice: 11000, lastPrice: 11000, high: 11000, low: 11000, count: 1, base: big.NewInt(2000), quote: big.NewInt(4000)}
	c := statsRow{firstSeq: 10, lastSeq: 10, firstPrice: 10000, lastPrice: 10000, high: 10000, low: 10000, count: 1, base: big.NewInt(1000), quote: big.NewInt(2000)}
	var totals statsTotals
	for _, row := range []statsRow{a, b, c} {
		if err := totals.add(row); err != nil {
			t.Fatal(err)
		}
	}
	want, err := statistics.Stats24h("BTC-USDT", now, trades)
	if err != nil {
		t.Fatal(err)
	}
	if got := totals.message("BTC-USDT", now); !proto.Equal(got, want) {
		t.Fatalf("exact rolling stats diverged from canonical source-order algorithm:\ngot: %v\nwant: %v", got, want)
	}
}
func TestStatsLargeExactVolumesAndOverflow(t *testing.T) {
	max := new(big.Int).SetUint64(math.MaxUint64)
	r := statsRow{firstSeq: 1, lastSeq: 1, firstPrice: math.MaxUint64, lastPrice: math.MaxUint64,
		high: math.MaxUint64, low: math.MaxUint64, base: max, quote: max, count: 1}
	var totals statsTotals
	if err := totals.add(r); err != nil {
		t.Fatal(err)
	}
	r.firstSeq, r.lastSeq = 2, 2
	if err := totals.add(r); err != nil {
		t.Fatal(err)
	}
	want := new(big.Int).Mul(max, big.NewInt(2)).String()
	got := totals.message("BTC-USDT", time.Now())
	if got.GetBaseVolumeAtoms() != want || got.GetQuoteVolumeAtoms() != want || got.GetPriceChangeBps() != "" {
		t.Fatalf("overflow or fractional statistics: %v", got)
	}
	totals.count = math.MaxUint64
	if err := totals.add(r); err == nil {
		t.Fatal("trade count uint64 overflow accepted")
	}
}
func TestStatsRowRejectsMalformedAndPrecisionLoss(t *testing.T) {
	v := [11]string{"9007199254740993", "0", "10000", "9007199254740994", "0", "11000", "11000", "10000", "100000000000000000000", "100000000000000000000", "2"}
	r, err := decodeStatsRow(v)
	if err != nil || r.firstSeq != 9007199254740993 || r.lastSeq != 9007199254740994 {
		t.Fatalf("high sequence was rounded or rejected: %+v %v", r, err)
	}
	v[6] = "9999" // high lower than low
	if _, err := decodeStatsRow(v); err == nil {
		t.Fatal("invalid price bounds accepted")
	}
	v[6] = "11000"
	v[8] = "1.25" // exact integer representation only
	if _, err := decodeStatsRow(v); err == nil {
		t.Fatal("fractional atoms accepted")
	}
	v[8] = "100000000000000000000"
	v[0] = "18446744073709551616"
	if _, err := decodeStatsRow(v); err == nil {
		t.Fatal("out-of-range market sequence accepted")
	}
}

func BenchmarkStatsExactMerge1440Minutes(b *testing.B) {
	rows := make([]statsRow, 1440)
	for i := range rows {
		n := uint64(i + 1)
		rows[i] = statsRow{firstSeq: n, lastSeq: n, firstPrice: 10000, lastPrice: 10000,
			high: 10000, low: 10000, count: 100, base: big.NewInt(100000), quote: big.NewInt(200000)}
	}
	b.ReportAllocs()

	for b.Loop() {
		var totals statsTotals
		for _, row := range rows {
			if err := totals.add(row); err != nil {
				b.Fatal(err)
			}
		}
		_ = totals.message("BTC-USDT", time.Now())
	}
}
