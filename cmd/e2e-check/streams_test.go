package main

import (
	"reflect"
	"testing"
	"time"

	market "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/book"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/trade"
)

// The E2E checker publishes real committed matching results, not raw trade
// rows. Validate their realism with the exact production projectors before
// depending on them as an acceptance fixture.
func TestCEX265MatchingFixtureAndBookDeltas(t *testing.T) {
	def := &md.MarketProjectionDefinition{
		SourceMarketId: sourceID, DefinitionVersion: definition,
		Market: &market.Market{MarketId: marketSymbol, BaseAssetScale: 8, QuoteAssetScale: 6,
			PriceScale: 2, PriceTickUnits: 25, QuantityStepBaseAtoms: 1000},
	}
	projector, err := book.New(def, 20)
	if err != nil {
		t.Fatal(err)
	}
	var view *bookView
	for seq := uint64(1); seq <= 5; seq++ {
		input := envelope(seq, time.Unix(1700000000, 0).UTC())
		delta, err := projector.Apply(input)
		if err != nil {
			t.Fatalf("matching result %d is not a valid book projection: %v", seq, err)
		}
		if delta == nil || delta.GetMarketSequence() != seq || delta.GetPreviousMarketSequence() != seq-1 {
			t.Fatalf("unexpected delta for seq=%d: %v", seq, delta)
		}
		ticks, err := trade.Project(input, def)
		if err != nil {
			t.Fatalf("trade projection seq=%d rejected (quote must be exact): %v", seq, err)
		}
		switch seq {
		case 1, 4:
			if len(ticks) != 0 {
				t.Fatalf("resting order seq=%d created %d trades", seq, len(ticks))
			}
		case 2, 3:
			if len(ticks) != 1 {
				t.Fatalf("seq=%d should create one trade", seq)
			}
		case 5:
			if len(ticks) != 2 {
				t.Fatalf("seq5 must cross two maker orders; got %d trades", len(ticks))
			}
			for ordinal, tick := range ticks {
				if err := expectTradeTick(tick, 5, uint64(ordinal)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if seq == 3 {
			view, err = viewFromSnapshot(projector.Snapshot(), 3)
			if err != nil {
				t.Fatal(err)
			}
		} else if seq >= 4 {
			if err := view.apply(delta); err != nil {
				t.Fatalf("book delta %d did not produce the correct materialized view: %v", seq, err)
			}
		}
	}
	if view.sequence != 5 || !reflect.DeepEqual(view.asks, map[uint64]string{}) {
		t.Fatalf("final L2 view not fully crossed: %+v", view)
	}
}

func TestCEX265RejectsGapAndRegression(t *testing.T) {
	v := &bookView{sequence: 3, bids: map[uint64]string{}, asks: map[uint64]string{price: "2000"}}
	for _, d := range []*md.BookDelta{
		{MarketId: marketSymbol, SourceMarketId: sourceID, MarketDefinitionVersion: definition,
			Depth: 20, PreviousMarketSequence: 3, MarketSequence: 5},
		{MarketId: marketSymbol, SourceMarketId: sourceID, MarketDefinitionVersion: definition,
			Depth: 20, PreviousMarketSequence: 2, MarketSequence: 4},
	} {
		if err := v.apply(d); err == nil {
			t.Fatalf("accepted noncontiguous delta %v", d)
		}
	}
	if v.sequence != 3 {
		t.Fatalf("bad delta advanced sequence: %d", v.sequence)
	}
}

func TestCEX265TickerFixtureRejectsStaleOrWrongState(t *testing.T) {
	for _, seq := range []uint64{3, 4, 5} {
		ticker := &md.Ticker{MarketId: marketSymbol, MarketSequence: seq}
		if expectTickerState(ticker, seq) == nil {
			t.Fatalf("accepted ticker without timestamp or exact prices at seq=%d", seq)
		}
	}
}
