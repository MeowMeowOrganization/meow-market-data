package l2

import (
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bookcheck"
	"testing"
)

func BenchmarkApply20Levels(b *testing.B) {
	expected := bookcheck.Expected{SourceMarketID: 7, MarketID: "BTC-USDT", DefinitionVersion: 3, Depth: 20, PriceTickUnits: 25, QuantityStepBaseAtoms: 1000}
	snap := &md.BookSnapshot{SourceMarketId: 7, MarketId: "BTC-USDT", MarketDefinitionVersion: 3, MarketSequence: 1, Depth: 20}
	for i := uint64(0); i < 20; i++ {
		snap.Bids = append(snap.Bids, &md.BookLevel{PriceUnits: 10000 - i*25, BaseQuantityAtoms: "1000"})
		snap.Asks = append(snap.Asks, &md.BookLevel{PriceUnits: 10100 + i*25, BaseQuantityAtoms: "1000"})
	}
	delta := &md.BookDelta{SourceMarketId: 7, MarketId: "BTC-USDT", MarketDefinitionVersion: 3, PreviousMarketSequence: 1, MarketSequence: 2, Depth: 20,
		BidUpdates: []*md.BookLevelUpdate{{PriceUnits: 10000, BaseQuantityAtoms: "2000"}},
		AskUpdates: []*md.BookLevelUpdate{{PriceUnits: 10100, BaseQuantityAtoms: "2000"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		after, err := Apply(snap, delta, expected)
		if err != nil || after.GetMarketSequence() != 2 {
			b.Fatalf("projection: %v", err)
		}
	}
}
