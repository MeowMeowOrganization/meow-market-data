// Package l2 is the pure, deterministic top-of-book delta applier shared by
// Redis projection/recovery. It never depends on Redis or the durable journal.
package l2

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bookcheck"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidDelta = errors.New("l2: invalid book delta")
	ErrSequenceGap  = errors.New("l2: noncontiguous market sequence")
)

// ValidateDelta ensures a single canonical matching sequence and well-formed
// top-N changes. No floats, signed sequence conversions or implicit truncation.
func ValidateDelta(d *md.BookDelta, want bookcheck.Expected) error {
	if d == nil || d.GetMarketId() != want.MarketID || d.GetSourceMarketId() != want.SourceMarketID ||
		d.GetMarketDefinitionVersion() != want.DefinitionVersion || d.GetDepth() != want.Depth ||
		d.GetMarketSequence() == 0 || d.GetPreviousMarketSequence() == math.MaxUint64 ||
		d.GetMarketSequence() != d.GetPreviousMarketSequence()+1 || want.Depth == 0 ||
		want.MarketID == "" || want.DefinitionVersion == 0 || want.PriceTickUnits == 0 || want.QuantityStepBaseAtoms == 0 {
		return ErrInvalidDelta
	}
	check := func(updates []*md.BookLevelUpdate, descending bool) error {
		var last uint64
		for i, update := range updates {
			if update == nil || update.GetPriceUnits() == 0 || update.GetPriceUnits()%want.PriceTickUnits != 0 ||
				(i > 0 && ((descending && update.GetPriceUnits() >= last) || (!descending && update.GetPriceUnits() <= last))) {
				return ErrInvalidDelta
			}
			atoms, ok := bookcheck.NonnegativeDecimal(update.GetBaseQuantityAtoms(), true)
			if !ok || new(big.Int).Mod(atoms, new(big.Int).SetUint64(want.QuantityStepBaseAtoms)).Sign() != 0 {
				return ErrInvalidDelta
			}
			last = update.GetPriceUnits()
		}
		return nil
	}
	if err := check(d.GetBidUpdates(), true); err != nil {
		return err
	}
	return check(d.GetAskUpdates(), false)
}

// Apply checks the immutable predecessor, applies one complete delta, and
// verifies that the result is itself a valid top-N snapshot. A no-op delta
// still advances the sequence. Caller-owned protobufs are never mutated.
func Apply(before *md.BookSnapshot, delta *md.BookDelta, want bookcheck.Expected) (*md.BookSnapshot, error) {
	if err := bookcheck.Validate(before, want); err != nil {
		return nil, fmt.Errorf("%w: predecessor: %v", ErrInvalidDelta, err)
	}
	if err := ValidateDelta(delta, want); err != nil {
		return nil, err
	}
	if before.GetMarketSequence() != delta.GetPreviousMarketSequence() {
		return nil, fmt.Errorf("%w: current=%d previous=%d", ErrSequenceGap, before.GetMarketSequence(), delta.GetPreviousMarketSequence())
	}
	applySide := func(levels []*md.BookLevel, updates []*md.BookLevelUpdate, bid bool) ([]*md.BookLevel, error) {
		byPrice := make(map[uint64]string, len(levels)+len(updates))
		for _, level := range levels {
			byPrice[level.GetPriceUnits()] = level.GetBaseQuantityAtoms()
		}
		for _, update := range updates {
			p, qty := update.GetPriceUnits(), update.GetBaseQuantityAtoms()
			if qty == "0" {
				if _, exists := byPrice[p]; !exists {
					return nil, ErrInvalidDelta
				}
				delete(byPrice, p)
			} else {
				byPrice[p] = qty
			}
		}
		// All levels evicted from top-N MUST be explicitly removed by the
		// journal. Silently trimming an oversized result could hide divergence.
		if len(byPrice) > int(want.Depth) {
			return nil, ErrInvalidDelta
		}
		prices := make([]uint64, 0, len(byPrice))
		for p := range byPrice {
			prices = append(prices, p)
		}
		sort.Slice(prices, func(i, j int) bool {
			if bid {
				return prices[i] > prices[j]
			}
			return prices[i] < prices[j]
		})
		out := make([]*md.BookLevel, 0, len(prices))
		for _, p := range prices {
			out = append(out, &md.BookLevel{PriceUnits: p, BaseQuantityAtoms: byPrice[p]})
		}
		return out, nil
	}
	bids, err := applySide(before.GetBids(), delta.GetBidUpdates(), true)
	if err != nil {
		return nil, fmt.Errorf("%w: bids: %v", ErrInvalidDelta, err)
	}
	asks, err := applySide(before.GetAsks(), delta.GetAskUpdates(), false)
	if err != nil {
		return nil, fmt.Errorf("%w: asks: %v", ErrInvalidDelta, err)
	}
	after := proto.Clone(before).(*md.BookSnapshot)
	after.MarketSequence, after.Bids, after.Asks = delta.GetMarketSequence(), bids, asks
	if err := bookcheck.Validate(after, want); err != nil {
		return nil, fmt.Errorf("%w: resulting book: %v", ErrInvalidDelta, err)
	}
	return after, nil
}
