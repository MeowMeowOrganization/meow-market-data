package redis

import (
	"errors"
	"fmt"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bookcheck"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
)

var ErrInvalidBundle = errors.New("redis: invalid projection bundle")

// ValidateBundle checks the internal publication boundary before any Redis
// mutation. Source generation is independently carried by Cache.Namespace.
// The read API applies additional market-registry tick/lot checks.
func ValidateBundle(b *readmodel.Bundle) error {
	bad := func(why string) error { return fmt.Errorf("%w: %s", ErrInvalidBundle, why) }
	if b == nil || b.Snapshot == nil || b.Ticker == nil || b.Stats == nil || b.AsOf.IsZero() ||
		b.AsOf.Before(time.Unix(0, 0)) || b.SourceMarketID != b.Snapshot.GetSourceMarketId() {
		return bad("missing/inconsistent identity or timestamp")
	}
	s := b.Snapshot
	if err := bookcheck.Validate(s, bookcheck.Expected{SourceMarketID: b.SourceMarketID, MarketID: s.GetMarketId(),
		DefinitionVersion: s.GetMarketDefinitionVersion(), Depth: s.GetDepth()}); err != nil {
		return fmt.Errorf("%w: book: %v", ErrInvalidBundle, err)
	}
	if err := bookcheck.ValidateTicker(b.Ticker, s); err != nil {
		return bad("ticker does not match snapshot")
	}
	if err := bookcheck.ValidateStats(b.Stats, s.GetMarketId(), b.AsOf); err != nil ||
		!b.Stats.GetAsOfInclusive().AsTime().Equal(b.AsOf) {
		return bad("stats identity/window")
	}
	if len(b.LatestTrades) > readmodel.RecentTradesLimit {
		return bad("too many recent trades")
	}
	var previousSequence, previousOrdinal uint64
	for i, trade := range b.LatestTrades {
		if trade == nil || trade.GetMarketId() != s.GetMarketId() || trade.GetSourceMarketId() != b.SourceMarketID ||
			trade.GetTradeId() == "" || trade.GetMatchSequence() == 0 || trade.GetMatchSequence() > s.GetMarketSequence() ||
			trade.GetPriceUnits() == 0 || trade.GetBaseQuantityAtoms() == 0 || trade.GetQuoteQuantityAtoms() == 0 ||
			trade.GetRecordedAt() == nil || trade.GetRecordedAt().CheckValid() != nil {
			return bad("invalid recent trade")
		}
		if i > 0 && (trade.GetMatchSequence() > previousSequence ||
			(trade.GetMatchSequence() == previousSequence && trade.GetEventOrdinal() >= previousOrdinal)) {
			return bad("trades not strictly newest-first")
		}
		previousSequence, previousOrdinal = trade.GetMatchSequence(), trade.GetEventOrdinal()
	}
	for interval, candles := range b.Candles {
		if !supportedInterval(interval) || len(candles) > readmodel.RecentCandlesLimit {
			return bad("unsupported or oversized candles")
		}
		for i, candle := range candles {
			if candle == nil || candle.GetMarketId() != s.GetMarketId() || candle.GetInterval() != interval ||
				candle.GetStartTime() == nil || candle.GetEndTime() == nil ||
				candle.GetStartTime().CheckValid() != nil || candle.GetEndTime().CheckValid() != nil ||
				!candle.GetStartTime().AsTime().Before(candle.GetEndTime().AsTime()) ||
				candle.GetLowPriceUnits() == 0 || candle.GetLowPriceUnits() > candle.GetHighPriceUnits() ||
				candle.GetOpenPriceUnits() < candle.GetLowPriceUnits() || candle.GetOpenPriceUnits() > candle.GetHighPriceUnits() ||
				candle.GetClosePriceUnits() < candle.GetLowPriceUnits() || candle.GetClosePriceUnits() > candle.GetHighPriceUnits() ||
				candle.GetTradeCount() == 0 {
				return bad("invalid candle")
			}
			if _, ok := bookcheck.NonnegativeDecimal(candle.GetBaseVolumeAtoms(), false); !ok {
				return bad("candle base volume")
			}
			if _, ok := bookcheck.NonnegativeDecimal(candle.GetQuoteVolumeAtoms(), false); !ok {
				return bad("candle quote volume")
			}
			if i > 0 && !candles[i-1].GetStartTime().AsTime().After(candle.GetStartTime().AsTime()) {
				return bad("candles not newest-first")
			}
		}
	}
	return nil
}

func supportedInterval(v md.CandleInterval) bool {
	switch v {
	case md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES,
		md.CandleInterval_CANDLE_INTERVAL_FIFTEEN_MINUTES, md.CandleInterval_CANDLE_INTERVAL_ONE_HOUR,
		md.CandleInterval_CANDLE_INTERVAL_ONE_DAY:
		return true
	}
	return false
}
