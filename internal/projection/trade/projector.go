// Package trade projects only public executions from a validated committed
// matcher result. All monetary arithmetic is exact and overflow-checked.
package trade

import (
	"errors"
	"fmt"
	"math/big"

	marketdatav1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matchingv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrUnrepresentable = errors.New("trade: unrepresentable exact amount")
var maxUint64 = new(big.Int).SetUint64(^uint64(0))

// ID is a deterministic canonicalization proposal of MatchingEventId.
// Verify it matches the existing Order Service / Accounting formatter before
// connecting to the real publisher; protobuf contracts do not specify text ID.
func ID(marketID uint32, sequence, ordinal uint64) string {
	return fmt.Sprintf("%d:%d:%d", marketID, sequence, ordinal)
}

func Project(input *matchingv1.CommittedMatchingResult, def *marketdatav1.MarketProjectionDefinition) ([]*marketdatav1.TradeTick, error) {
	if input == nil || input.GetResult() == nil || input.GetRecordedAt() == nil || def == nil || def.GetMarket() == nil {
		return nil, errors.New("trade: missing result, recorded_at or definition")
	}
	if err := input.GetRecordedAt().CheckValid(); err != nil {
		return nil, err
	}
	m := def.GetMarket()
	seq := input.GetResult().GetMarketSequence()
	if seq == 0 || input.GetResult().GetCommand() == nil || input.GetResult().GetCommand().GetMarketId() != def.GetSourceMarketId() ||
		input.GetMarketDefinitionVersion() != def.GetDefinitionVersion() || m.GetMarketId() == "" ||
		m.GetPriceTickUnits() == 0 || m.GetQuantityStepBaseAtoms() == 0 {
		return nil, errors.New("trade: inconsistent market definition")
	}
	// Restrict exponent magnitudes to avoid adversarial allocation from bad
	// market reference data. Real asset scales are dramatically lower.
	if m.GetBaseAssetScale() > 38 || m.GetQuoteAssetScale() > 38 || m.GetPriceScale() > 38 {
		return nil, errors.New("trade: unsupported asset/price scale > 38")
	}
	ten := big.NewInt(10)
	numeratorScale := new(big.Int).Exp(ten, big.NewInt(int64(m.GetQuoteAssetScale())), nil)
	denominatorScale := new(big.Int).Exp(ten, big.NewInt(int64(m.GetBaseAssetScale()+m.GetPriceScale())), nil)
	out := make([]*marketdatav1.TradeTick, 0)
	for i, event := range input.GetResult().GetEvents() {
		if event == nil || event.GetEventId() == nil {
			return nil, errors.New("trade: missing event identity")
		}
		eid := event.GetEventId()
		if eid.GetMarketId() != def.GetSourceMarketId() || eid.GetMarketSequence() != seq || eid.GetEventOrdinal() != uint64(i) {
			return nil, fmt.Errorf("trade: mismatched event identity at ordinal %d", i)
		}
		ex := event.GetExectuion() // inherited CEX matching proto field spelling
		if ex == nil {
			continue
		}
		if ex.GetQuantityLots() <= 0 || ex.GetPriceTicks() <= 0 ||
			(ex.GetTakerSide() != orderv1.OrderSide_ORDER_SIDE_BUY && ex.GetTakerSide() != orderv1.OrderSide_ORDER_SIDE_SELL) {
			return nil, errors.New("trade: invalid execution")
		}
		price := new(big.Int).Mul(big.NewInt(ex.GetPriceTicks()), new(big.Int).SetUint64(m.GetPriceTickUnits()))
		base := new(big.Int).Mul(big.NewInt(ex.GetQuantityLots()), new(big.Int).SetUint64(m.GetQuantityStepBaseAtoms()))
		quoteNumerator := new(big.Int).Mul(price, base)
		quoteNumerator.Mul(quoteNumerator, numeratorScale)
		quote, remainder := new(big.Int).QuoRem(quoteNumerator, denominatorScale, new(big.Int))
		if remainder.Sign() != 0 || price.Sign() <= 0 || base.Sign() <= 0 || quote.Sign() <= 0 ||
			price.Cmp(maxUint64) > 0 || base.Cmp(maxUint64) > 0 || quote.Cmp(maxUint64) > 0 {
			return nil, fmt.Errorf("%w: market=%d seq=%d ordinal=%d", ErrUnrepresentable, eid.GetMarketId(), seq, i)
		}
		out = append(out, &marketdatav1.TradeTick{
			TradeId: ID(eid.GetMarketId(), seq, eid.GetEventOrdinal()), MarketId: m.GetMarketId(),
			PriceUnits: price.Uint64(), BaseQuantityAtoms: base.Uint64(), QuoteQuantityAtoms: quote.Uint64(),
			AggressorSide: ex.GetTakerSide(), MatchSequence: seq, EventOrdinal: eid.GetEventOrdinal(),
			SourceMarketId: def.GetSourceMarketId(), MarketDefinitionVersion: def.GetDefinitionVersion(),
			RecordedAt: timestamppb.New(input.GetRecordedAt().AsTime()),
		})
	}
	return out, nil
}
