package redis

import (
	"errors"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func sampleBundle(id uint32, seq uint64, at time.Time) *readmodel.Bundle {
	bid, ask := uint64(10000), uint64(10100)
	return &readmodel.Bundle{SourceMarketID: id, AsOf: at,
		Snapshot: &md.BookSnapshot{MarketId: "BTC-USDT", SourceMarketId: id, MarketSequence: seq, MarketDefinitionVersion: 3, Depth: 20,
			Bids: []*md.BookLevel{{PriceUnits: bid, BaseQuantityAtoms: "2000"}},
			Asks: []*md.BookLevel{{PriceUnits: ask, BaseQuantityAtoms: "1000"}}},
		Ticker: &md.Ticker{MarketId: "BTC-USDT", MarketSequence: seq, BestBidPriceUnits: &bid, BestAskPriceUnits: &ask, UpdatedAt: timestamppb.New(at)},
		Stats:  &md.Stats24H{MarketId: "BTC-USDT", WindowStartExclusive: timestamppb.New(at.Add(-24 * time.Hour)), AsOfInclusive: timestamppb.New(at), BaseVolumeAtoms: "0", QuoteVolumeAtoms: "0"},
	}
}

func TestValidateBundleRejectsIncoherentViews(t *testing.T) {
	at := time.Now().UTC()
	good := sampleBundle(7, 9, at)
	if err := ValidateBundle(good); err != nil {
		t.Fatalf("valid bundle rejected: %v", err)
	}
	cases := map[string]func(*readmodel.Bundle){
		"ticker sequence":          func(b *readmodel.Bundle) { b.Ticker.MarketSequence++ },
		"ticker bid":               func(b *readmodel.Bundle) { n := uint64(123); b.Ticker.BestBidPriceUnits = &n },
		"ticker market":            func(b *readmodel.Bundle) { b.Ticker.MarketId = "X-USDT" },
		"book crossed":             func(b *readmodel.Bundle) { b.Snapshot.Asks[0].PriceUnits = 9999 },
		"stats market":             func(b *readmodel.Bundle) { b.Stats.MarketId = "X-USDT" },
		"stats asof":               func(b *readmodel.Bundle) { b.Stats.AsOfInclusive = timestamppb.New(at.Add(-time.Hour)) },
		"negative stats volume":    func(b *readmodel.Bundle) { b.Stats.BaseVolumeAtoms = "-1" },
		"missing ticker timestamp": func(b *readmodel.Bundle) { b.Ticker.UpdatedAt = nil },
		"missing stats":            func(b *readmodel.Bundle) { b.Stats = nil },
		"wrong source id":          func(b *readmodel.Bundle) { b.SourceMarketID++ },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			b := &readmodel.Bundle{SourceMarketID: good.SourceMarketID, AsOf: good.AsOf,
				Snapshot: proto.Clone(good.Snapshot).(*md.BookSnapshot),
				Ticker:   proto.Clone(good.Ticker).(*md.Ticker),
				Stats:    proto.Clone(good.Stats).(*md.Stats24H)}
			mutate(b)
			if err := ValidateBundle(b); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("expected invalid bundle, got %v", err)
			}
		})
	}
}
