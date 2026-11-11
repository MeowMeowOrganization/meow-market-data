package bookcheck

import (
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
)

func ValidateTicker(t *md.Ticker, s *md.BookSnapshot) error {
	if t == nil || s == nil || t.GetMarketId() != s.GetMarketId() || t.GetMarketSequence() != s.GetMarketSequence() ||
		t.GetUpdatedAt() == nil || t.GetUpdatedAt().CheckValid() != nil ||
		(t.LastPriceUnits != nil && t.GetLastPriceUnits() == 0) {
		return ErrInvalid
	}
	if len(s.GetBids()) == 0 {
		if t.BestBidPriceUnits != nil {
			return ErrInvalid
		}
	} else if t.BestBidPriceUnits == nil || t.GetBestBidPriceUnits() != s.GetBids()[0].GetPriceUnits() {
		return ErrInvalid
	}
	if len(s.GetAsks()) == 0 {
		if t.BestAskPriceUnits != nil {
			return ErrInvalid
		}
	} else if t.BestAskPriceUnits == nil || t.GetBestAskPriceUnits() != s.GetAsks()[0].GetPriceUnits() {
		return ErrInvalid
	}
	return nil
}

// ValidateStats checks the cached rolling-window format and clock bounds. It
// does not claim source synchronization, which requires separate lag metrics.
func ValidateStats(st *md.Stats24H, market string, now time.Time) error {
	if st == nil || st.GetMarketId() != market || st.GetWindowStartExclusive() == nil || st.GetAsOfInclusive() == nil ||
		st.GetWindowStartExclusive().CheckValid() != nil || st.GetAsOfInclusive().CheckValid() != nil ||
		!st.GetWindowStartExclusive().AsTime().Add(24*time.Hour).Equal(st.GetAsOfInclusive().AsTime()) ||
		!st.GetAsOfInclusive().AsTime().After(now.Add(-20*time.Second)) ||
		st.GetAsOfInclusive().AsTime().After(now.Add(5*time.Second)) {
		return ErrInvalid
	}
	base, ok := NonnegativeDecimal(st.GetBaseVolumeAtoms(), true)
	if !ok {
		return ErrInvalid
	}
	quote, ok := NonnegativeDecimal(st.GetQuoteVolumeAtoms(), true)
	if !ok {
		return ErrInvalid
	}
	if st.GetTradeCount() == 0 {
		if base.Sign() != 0 || quote.Sign() != 0 || st.OpenPriceUnits != nil || st.HighPriceUnits != nil ||
			st.LowPriceUnits != nil || st.ClosePriceUnits != nil || st.GetPriceChangeBps() != "" {
			return ErrInvalid
		}
		return nil
	}
	if base.Sign() == 0 || quote.Sign() == 0 || st.OpenPriceUnits == nil || st.HighPriceUnits == nil ||
		st.LowPriceUnits == nil || st.ClosePriceUnits == nil || st.GetLowPriceUnits() == 0 ||
		st.GetHighPriceUnits() < st.GetLowPriceUnits() ||
		st.GetOpenPriceUnits() < st.GetLowPriceUnits() || st.GetOpenPriceUnits() > st.GetHighPriceUnits() ||
		st.GetClosePriceUnits() < st.GetLowPriceUnits() || st.GetClosePriceUnits() > st.GetHighPriceUnits() {
		return ErrInvalid
	}
	return nil
}
