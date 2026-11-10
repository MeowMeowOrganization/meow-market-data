package book

import (
	"math/big"
	"sort"

	marketdatav1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
)

type visibleLevel struct {
	price uint64
	atoms string
}

// visibleLevels computes the complete visible top-N from immutable base state
// overlaid with a single in-flight result. Iteration order of Go maps cannot
// affect output ordering: all ticks and all emitted changes are sorted.
func visibleLevels(base, changes map[int64]*big.Int, depth uint32, bid bool, tickUnits, lotAtoms uint64) []visibleLevel {
	prices := make([]int64, 0, len(base)+len(changes))
	for ticks, count := range base {
		if replacement, modified := changes[ticks]; modified {
			count = replacement
		}
		if count.Sign() > 0 {
			prices = append(prices, ticks)
		}
	}
	for ticks, count := range changes {
		if _, exists := base[ticks]; !exists && count.Sign() > 0 {
			prices = append(prices, ticks)
		}
	}
	sort.Slice(prices, func(i, j int) bool {
		if bid {
			return prices[i] > prices[j]
		}
		return prices[i] < prices[j]
	})
	if uint64(len(prices)) > uint64(depth) {
		prices = prices[:int(depth)]
	}

	out := make([]visibleLevel, 0, len(prices))
	for _, ticks := range prices {
		count := base[ticks]
		if replacement, touched := changes[ticks]; touched {
			count = replacement
		}
		atoms := new(big.Int).Mul(count, new(big.Int).SetUint64(lotAtoms))
		// priceFits was checked on every accepted placement. Matching events
		// only refer to already validated maker prices.
		out = append(out, visibleLevel{price: uint64(ticks) * tickUnits, atoms: atoms.String()})
	}
	return out
}

func diffVisible(before, after []visibleLevel, bid bool) []*marketdatav1.BookLevelUpdate {
	oldQty := make(map[uint64]string, len(before))
	newQty := make(map[uint64]string, len(after))
	for _, level := range before {
		oldQty[level.price] = level.atoms
	}
	for _, level := range after {
		newQty[level.price] = level.atoms
	}

	// A changed level that falls outside depth is not published. On a top-N
	// membership change we also emit removals for levels evicted from top-N,
	// even if those levels still have resting liquidity in the full book.
	prices := make([]uint64, 0, len(before)+len(after))
	for price, old := range oldQty {
		if now, exists := newQty[price]; !exists || now != old {
			prices = append(prices, price)
		}
	}
	for price := range newQty {
		if _, exists := oldQty[price]; !exists {
			prices = append(prices, price)
		}
	}
	sort.Slice(prices, func(i, j int) bool {
		if bid {
			return prices[i] > prices[j]
		}
		return prices[i] < prices[j]
	})
	updates := make([]*marketdatav1.BookLevelUpdate, 0, len(prices))
	for _, price := range prices {
		qty, exists := newQty[price]
		if !exists {
			qty = "0"
		}
		updates = append(updates, &marketdatav1.BookLevelUpdate{PriceUnits: price, BaseQuantityAtoms: qty})
	}
	return updates
}

func (p *Projector) snapshotLocked() *marketdatav1.BookSnapshot {
	visibleBids := visibleLevels(p.bids, nil, p.depth, true, p.tickUnits, p.lotAtoms)
	visibleAsks := visibleLevels(p.asks, nil, p.depth, false, p.tickUnits, p.lotAtoms)
	snapshot := &marketdatav1.BookSnapshot{
		MarketId:                p.symbol,
		SourceMarketId:          p.sourceMarketID,
		MarketSequence:          p.sequence,
		MarketDefinitionVersion: p.definitionVersion,
		Depth:                   p.depth,
		Bids:                    make([]*marketdatav1.BookLevel, 0, len(visibleBids)),
		Asks:                    make([]*marketdatav1.BookLevel, 0, len(visibleAsks)),
	}
	for _, level := range visibleBids {
		snapshot.Bids = append(snapshot.Bids, &marketdatav1.BookLevel{PriceUnits: level.price, BaseQuantityAtoms: level.atoms})
	}
	for _, level := range visibleAsks {
		snapshot.Asks = append(snapshot.Asks, &marketdatav1.BookLevel{PriceUnits: level.price, BaseQuantityAtoms: level.atoms})
	}
	return snapshot
}
