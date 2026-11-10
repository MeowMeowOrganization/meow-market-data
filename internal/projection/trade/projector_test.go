package trade

import (
	"errors"
	"math"
	"testing"
	"time"

	marketv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matching "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	order "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func makeTradeResult() (*matching.CommittedMatchingResult, *md.MarketProjectionDefinition) {
	def := &md.MarketProjectionDefinition{SourceMarketId: 7, DefinitionVersion: 3, Market: &marketv1.Market{
		MarketId: "BTC-USDT", PriceTickUnits: 25, QuantityStepBaseAtoms: 1000,
		BaseAssetScale: 8, QuoteAssetScale: 6, PriceScale: 2,
	}}
	evt := func(ord uint64, lot int64) *matching.SequencedMatchingEvent {
		return &matching.SequencedMatchingEvent{
			EventId: &matching.MatchingEventId{MarketId: 7, MarketSequence: 42, EventOrdinal: ord},
			Event: &matching.SequencedMatchingEvent_Exectuion{Exectuion: &matching.Execution{
				MakerOrderId: 9 + ord, TakerOrderId: 10, TakerSide: order.OrderSide_ORDER_SIDE_BUY, PriceTicks: 100, QuantityLots: lot,
			}},
		}
	}
	r := &matching.CommittedMatchingResult{Result: &matching.MatchingResult{
		MarketSequence: 42, Command: &matching.MatchingCommand{MarketId: 7, CommandId: "buy"},
		Events: []*matching.SequencedMatchingEvent{evt(0, 1), evt(1, 2)},
	}, RecordedAt: timestamppb.New(time.Date(2026, 11, 10, 12, 34, 56, 123456789, time.UTC)), MarketDefinitionVersion: 3}
	return r, def
}
func TestExactPublicTradeProjectionMultiMaker(t *testing.T) {
	r, def := makeTradeResult()
	trades, err := Project(r, def)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 2 {
		t.Fatal(len(trades))
	}
	if trades[0].GetTradeId() != "7:42:0" || trades[1].GetTradeId() != "7:42:1" {
		t.Fatal("unstable ID")
	}
	if trades[0].GetPriceUnits() != 2500 || trades[0].GetBaseQuantityAtoms() != 1000 || trades[0].GetQuoteQuantityAtoms() != 250 {
		t.Fatalf("wrong exact conversion: %+v", trades[0])
	}
	if trades[1].GetQuoteQuantityAtoms() != 500 {
		t.Fatal("second maker quantity")
	}
	if trades[1].GetRecordedAt().AsTime().Nanosecond() != 123456789 {
		t.Fatal("nanoseconds lost")
	}
}
func TestNonIntegralOrOverflowFailsClosed(t *testing.T) {
	r, def := makeTradeResult()
	def.Market.PriceTickUnits = 1 // 100 units * 1000 atoms / 1e10 * 1e6 => 10 quote atoms, exact
	def.Market.PriceTickUnits = 0 // not a valid definition
	if _, err := Project(r, def); err == nil {
		t.Fatal("zero tick accepted")
	}
	def.Market.PriceTickUnits = 1
	r.Result.Events[0].GetExectuion().PriceTicks = 1 // 0.1 quote atom, cannot represent
	if _, err := Project(r, def); !errors.Is(err, ErrUnrepresentable) {
		t.Fatalf("nonintegral quote not rejected: %v", err)
	}
	r, def = makeTradeResult()
	def.Market.PriceTickUnits = math.MaxUint64
	if _, err := Project(r, def); !errors.Is(err, ErrUnrepresentable) {
		t.Fatalf("overflow not rejected: %v", err)
	}
}
func TestNoTradesForZeroEventPlacement(t *testing.T) {
	r, def := makeTradeResult()
	r.Result.Events = nil
	out, err := Project(r, def)
	if err != nil || len(out) != 0 {
		t.Fatal(err, len(out))
	}
}
