package bookcheck

import (
	"errors"
	"testing"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"google.golang.org/protobuf/proto"
)

func TestLevel2BookInvariants(t *testing.T) {
	market := Expected{SourceMarketID: 7, MarketID: "BTC-USDT", DefinitionVersion: 3, Depth: 3, PriceTickUnits: 25, QuantityStepBaseAtoms: 1000}
	good := &md.BookSnapshot{MarketId: market.MarketID, SourceMarketId: 7, MarketDefinitionVersion: 3, MarketSequence: 11, Depth: 3,
		Bids: []*md.BookLevel{{PriceUnits: 10000, BaseQuantityAtoms: "2000"}, {PriceUnits: 9975, BaseQuantityAtoms: "1000"}},
		Asks: []*md.BookLevel{{PriceUnits: 10025, BaseQuantityAtoms: "3000"}, {PriceUnits: 10050, BaseQuantityAtoms: "1000"}}}
	if err := Validate(good, market); err != nil {
		t.Fatalf("valid book: %v", err)
	}
	cases := map[string]func(*md.BookSnapshot){
		"wrong market":     func(b *md.BookSnapshot) { b.MarketId = "X-USDT" },
		"wrong definition": func(b *md.BookSnapshot) { b.MarketDefinitionVersion = 4 },
		"zero sequence":    func(b *md.BookSnapshot) { b.MarketSequence = 0 },
		"too many levels": func(b *md.BookSnapshot) {
			b.Bids = append(b.Bids, &md.BookLevel{PriceUnits: 9950, BaseQuantityAtoms: "1000"}, &md.BookLevel{PriceUnits: 9925, BaseQuantityAtoms: "1000"})
		},
		"duplicate bids":        func(b *md.BookSnapshot) { b.Bids[1].PriceUnits = b.Bids[0].PriceUnits },
		"out-of-order asks":     func(b *md.BookSnapshot) { b.Asks[1].PriceUnits = 10000 },
		"crossed":               func(b *md.BookSnapshot) { b.Asks[0].PriceUnits = 10000 },
		"zero price":            func(b *md.BookSnapshot) { b.Bids[0].PriceUnits = 0 },
		"not on tick":           func(b *md.BookSnapshot) { b.Bids[0].PriceUnits = 10001 },
		"zero quantity":         func(b *md.BookSnapshot) { b.Bids[0].BaseQuantityAtoms = "0" },
		"negative quantity":     func(b *md.BookSnapshot) { b.Bids[0].BaseQuantityAtoms = "-1000" },
		"noncanonical quantity": func(b *md.BookSnapshot) { b.Bids[0].BaseQuantityAtoms = "02000" },
		"not on lot":            func(b *md.BookSnapshot) { b.Asks[0].BaseQuantityAtoms = "1001" },
		"null level":            func(b *md.BookSnapshot) { b.Asks[0] = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			b := proto.Clone(good).(*md.BookSnapshot)
			mutate(b)
			if err := Validate(b, market); !errors.Is(err, ErrInvalid) {
				t.Fatalf("wanted ErrInvalid, got %v", err)
			}
		})
	}
}

func TestExactAtomDecimals(t *testing.T) {
	for _, s := range []string{"18446744073709551616000", "9999999999999999999999999999999999999999"} {
		if _, ok := NonnegativeDecimal(s, false); !ok {
			t.Fatalf("rejected exact integer %q", s)
		}
	}
	for _, s := range []string{"", "-1", "+1", "1.0", " 1", "00", "01", "1e5", "0"} {
		if _, ok := NonnegativeDecimal(s, false); ok {
			t.Fatalf("accepted invalid positive decimal %q", s)
		}
	}
	if _, ok := NonnegativeDecimal("0", true); !ok {
		t.Fatal("zero must be allowed for stats")
	}
}
