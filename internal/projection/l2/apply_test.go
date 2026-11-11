package l2

import (
	"errors"
	"math"
	"testing"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bookcheck"
	"google.golang.org/protobuf/proto"
)

var btc = bookcheck.Expected{SourceMarketID: 7, MarketID: "BTC-USDT", DefinitionVersion: 3, Depth: 2, PriceTickUnits: 25, QuantityStepBaseAtoms: 1000}

func book(seq uint64, bids, asks []*md.BookLevel) *md.BookSnapshot {
	return &md.BookSnapshot{SourceMarketId: 7, MarketId: "BTC-USDT", MarketSequence: seq, MarketDefinitionVersion: 3, Depth: 2, Bids: bids, Asks: asks}
}
func level(price uint64, qty string) *md.BookLevel {
	return &md.BookLevel{PriceUnits: price, BaseQuantityAtoms: qty}
}
func update(price uint64, qty string) *md.BookLevelUpdate {
	return &md.BookLevelUpdate{PriceUnits: price, BaseQuantityAtoms: qty}
}
func delta(prev uint64) *md.BookDelta {
	return &md.BookDelta{SourceMarketId: 7, MarketId: "BTC-USDT", MarketSequence: prev + 1, PreviousMarketSequence: prev, MarketDefinitionVersion: 3, Depth: 2}
}
func TestApplyTopDepthAndNoopDelta(t *testing.T) {
	before := book(1, []*md.BookLevel{level(2500, "2000"), level(2475, "3000")}, []*md.BookLevel{level(2600, "1000")})
	original := proto.Clone(before)
	d := delta(1)
	d.BidUpdates = []*md.BookLevelUpdate{update(2525, "1000"), update(2475, "0")}
	after, err := Apply(before, d, btc)
	if err != nil {
		t.Fatal(err)
	}
	want := book(2, []*md.BookLevel{level(2525, "1000"), level(2500, "2000")}, []*md.BookLevel{level(2600, "1000")})
	if !proto.Equal(after, want) {
		t.Fatalf("wrong top-N book: got=%v want=%v", after, want)
	}
	if !proto.Equal(original, before) {
		t.Fatal("mutated caller-owned predecessor")
	}
	noop := delta(2)
	next, err := Apply(after, noop, btc)
	if err != nil || next.GetMarketSequence() != 3 || !proto.Equal(next, book(3, want.GetBids(), want.GetAsks())) {
		t.Fatalf("no-op sequence: %v %v", next, err)
	}
}
func TestApplyRejectsInvalidDeltaWithoutMutation(t *testing.T) {
	before := book(5, []*md.BookLevel{level(2500, "1000")}, []*md.BookLevel{level(2600, "1000")})
	malformed := []struct {
		name   string
		change func(*md.BookDelta)
	}{
		{"gap", func(d *md.BookDelta) { d.PreviousMarketSequence = 3 }},
		{"jump", func(d *md.BookDelta) { d.MarketSequence = 7 }},
		{"definition", func(d *md.BookDelta) { d.MarketDefinitionVersion = 4 }},
		{"duplicate", func(d *md.BookDelta) {
			d.BidUpdates = []*md.BookLevelUpdate{update(2525, "1000"), update(2525, "2000")}
		}},
		{"unsorted", func(d *md.BookDelta) {
			d.BidUpdates = []*md.BookLevelUpdate{update(2500, "1000"), update(2525, "1000")}
		}},
		{"tick", func(d *md.BookDelta) { d.BidUpdates = []*md.BookLevelUpdate{update(2501, "1000")} }},
		{"step", func(d *md.BookDelta) { d.BidUpdates = []*md.BookLevelUpdate{update(2525, "1500")} }},
		{"negative", func(d *md.BookDelta) { d.BidUpdates = []*md.BookLevelUpdate{update(2525, "-1000")} }},
		{"redundant-removal", func(d *md.BookDelta) { d.BidUpdates = []*md.BookLevelUpdate{update(2400, "0")} }},
		{"missing-eviction", func(d *md.BookDelta) {
			d.BidUpdates = []*md.BookLevelUpdate{update(2550, "1000"), update(2525, "1000")}
		}},
		{"crossed", func(d *md.BookDelta) { d.BidUpdates = []*md.BookLevelUpdate{update(2600, "1000")} }},
	}
	original := proto.Clone(before)
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			d := delta(5)
			tc.change(d)
			if _, err := Apply(before, d, btc); err == nil {
				t.Fatal("accepted malformed delta")
			}
			if !proto.Equal(before, original) {
				t.Fatal("corrupted predecessor on failed apply")
			}
		})
	}
	noncontiguous := delta(4)
	if _, err := Apply(before, noncontiguous, btc); !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("expected sequence gap, got %v", err)
	}
}
func TestUint64SequencesBeyondJavascriptPrecision(t *testing.T) {
	const prev uint64 = 9007199254740993
	snap := book(prev, nil, nil)
	next, err := Apply(snap, delta(prev), btc)
	if err != nil || next.GetMarketSequence() != prev+1 {
		t.Fatalf("lost exact sequence: %+v %v", next, err)
	}
	max := delta(math.MaxUint64 - 1)
	if err := ValidateDelta(max, btc); err != nil {
		t.Fatal(err)
	}
	max.PreviousMarketSequence = math.MaxUint64
	if err := ValidateDelta(max, btc); !errors.Is(err, ErrInvalidDelta) {
		t.Fatal("overflow accepted")
	}
}
