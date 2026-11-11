// Package bookcheck validates the external Level-2 snapshot boundary without
// depending on Redis, database state or the matching engine implementation.
package bookcheck

import (
	"errors"
	"math/big"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
)

var ErrInvalid = errors.New("bookcheck: invalid order book")

type Expected struct {
	SourceMarketID        uint32
	MarketID              string
	DefinitionVersion     uint64
	Depth                 uint32
	PriceTickUnits        uint64
	QuantityStepBaseAtoms uint64
}

// NonnegativeDecimal accepts canonical unsigned base-ten values with no
// leading zeroes. Atom quantities can exceed uint64 after aggregation.
func NonnegativeDecimal(s string, allowZero bool) (*big.Int, bool) {
	if s == "" || len(s) > 128 || len(s) > 1 && s[0] == '0' {
		return nil, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return nil, false
		}
	}
	n, ok := new(big.Int).SetString(s, 10)
	return n, ok && (allowZero || n.Sign() > 0)
}

func Validate(snapshot *md.BookSnapshot, want Expected) error {
	if snapshot == nil || want.MarketID == "" || want.DefinitionVersion == 0 || want.Depth == 0 ||
		snapshot.GetMarketId() != want.MarketID || snapshot.GetSourceMarketId() != want.SourceMarketID ||
		snapshot.GetMarketDefinitionVersion() != want.DefinitionVersion || snapshot.GetDepth() != want.Depth ||
		snapshot.GetMarketSequence() == 0 || len(snapshot.GetBids()) > int(want.Depth) || len(snapshot.GetAsks()) > int(want.Depth) {
		return ErrInvalid
	}
	validateSide := func(levels []*md.BookLevel, descending bool) bool {
		var previous uint64
		for i, level := range levels {
			if level == nil || level.GetPriceUnits() == 0 {
				return false
			}
			price := level.GetPriceUnits()
			if want.PriceTickUnits != 0 && price%want.PriceTickUnits != 0 {
				return false
			}
			if i > 0 && ((descending && price >= previous) || (!descending && price <= previous)) {
				return false
			}
			atoms, ok := NonnegativeDecimal(level.GetBaseQuantityAtoms(), false)
			if !ok {
				return false
			}
			if want.QuantityStepBaseAtoms != 0 && new(big.Int).Mod(atoms, new(big.Int).SetUint64(want.QuantityStepBaseAtoms)).Sign() != 0 {
				return false
			}
			previous = price
		}
		return true
	}
	if !validateSide(snapshot.GetBids(), true) || !validateSide(snapshot.GetAsks(), false) {
		return ErrInvalid
	}
	if len(snapshot.GetBids()) != 0 && len(snapshot.GetAsks()) != 0 &&
		snapshot.GetBids()[0].GetPriceUnits() >= snapshot.GetAsks()[0].GetPriceUnits() {
		return ErrInvalid
	}
	return nil
}
