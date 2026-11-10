package book

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"

	marketdatav1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
)

var ErrInvalidCheckpoint = errors.New("book: invalid recovery checkpoint")

// MaxCheckpointBytes bounds the private persistent snapshot format. Raising
// this limit requires explicit load/restore sizing and operational testing.
const MaxCheckpointBytes = 128 << 20

// checkpointV1 is deliberately private and versioned. It is not a public L2
// snapshot: the per-order quantities are REQUIRED for future fills/cancels.
// Sorted orders make the JSON encoding deterministic and independently auditable.
// No floating-point fields appear in the representation.
type checkpointV1 struct {
	FormatVersion      uint32            `json:"format_version"`
	SourceMarketID     uint32            `json:"source_market_id"`
	MarketSymbol       string            `json:"market_symbol"`
	DefinitionVersion  uint64            `json:"definition_version"`
	Depth              uint32            `json:"depth"`
	TickUnits          uint64            `json:"tick_units"`
	LotAtoms           uint64            `json:"lot_atoms"`
	Sequence           uint64            `json:"sequence"`
	LastEnvelopeSHA256 string            `json:"last_envelope_sha256"`
	Orders             []checkpointOrder `json:"orders"`
}

type checkpointOrder struct {
	OrderID       uint64            `json:"order_id"`
	Side          orderv1.OrderSide `json:"side"`
	PriceTicks    int64             `json:"price_ticks"`
	RemainingLots int64             `json:"remaining_lots"`
}

// MarshalCheckpoint returns a complete, canonical private recovery state.
// The caller stores these bytes and a SHA-256 digest durably together with
// the Kafka partition cursor. Concurrent Snapshot/Apply is serialized.
func (p *Projector) MarshalCheckpoint() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	cp := checkpointV1{
		FormatVersion: 1, SourceMarketID: p.sourceMarketID,
		MarketSymbol: p.symbol, DefinitionVersion: p.definitionVersion,
		Depth: p.depth, TickUnits: p.tickUnits, LotAtoms: p.lotAtoms,
		Sequence: p.sequence, Orders: make([]checkpointOrder, 0, len(p.orders)),
	}
	if p.haveLastDigest {
		cp.LastEnvelopeSHA256 = hex.EncodeToString(p.lastDigest[:])
	}
	ids := make([]uint64, 0, len(p.orders))
	for id := range p.orders {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		o := p.orders[id]
		cp.Orders = append(cp.Orders, checkpointOrder{
			OrderID: id, Side: o.side, PriceTicks: o.priceTicks,
			RemainingLots: o.remainingLots,
		})
	}
	encoded, err := json.Marshal(cp)
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxCheckpointBytes {
		return nil, fmt.Errorf("%w: encoded state exceeds %d bytes", ErrInvalidCheckpoint, MaxCheckpointBytes)
	}
	return encoded, nil
}

// Restore creates a new projector from a durable checkpoint. Unlike an L2
// snapshot, this rebuilds the full resting-order index and all price levels.
// It validates the definition, sequence, digest and all book invariants before
// allowing callers to process another result.
func Restore(def *marketdatav1.MarketProjectionDefinition, depth uint32, data []byte) (*Projector, error) {
	p, err := New(def, depth)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > MaxCheckpointBytes { // bounded input; tune only with measured book sizes
		return nil, fmt.Errorf("%w: empty or oversized data", ErrInvalidCheckpoint)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var cp checkpointV1
	if err := dec.Decode(&cp); err != nil {
		return nil, fmt.Errorf("%w: decode: %v", ErrInvalidCheckpoint, err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing content", ErrInvalidCheckpoint)
	}
	if cp.FormatVersion != 1 || cp.SourceMarketID != p.sourceMarketID || cp.MarketSymbol != p.symbol ||
		cp.DefinitionVersion != p.definitionVersion || cp.Depth != p.depth ||
		cp.TickUnits != p.tickUnits || cp.LotAtoms != p.lotAtoms {
		return nil, fmt.Errorf("%w: schema or immutable definition mismatch", ErrInvalidCheckpoint)
	}
	if cp.Sequence == 0 {
		if len(cp.Orders) != 0 || cp.LastEnvelopeSHA256 != "" {
			return nil, fmt.Errorf("%w: nonempty initial state", ErrInvalidCheckpoint)
		}
	} else {
		decoded, err := hex.DecodeString(cp.LastEnvelopeSHA256)
		if err != nil || len(decoded) != sha256.Size {
			return nil, fmt.Errorf("%w: invalid last envelope digest", ErrInvalidCheckpoint)
		}
		copy(p.lastDigest[:], decoded)
		p.haveLastDigest = true
	}
	stage := newStage(p)
	var prev uint64
	for i, o := range cp.Orders {
		if (i > 0 && o.OrderID <= prev) || !validSide(o.Side) || o.PriceTicks <= 0 ||
			o.RemainingLots <= 0 || !priceFits(o.PriceTicks, p.tickUnits) {
			return nil, fmt.Errorf("%w: invalid order at index=%d", ErrInvalidCheckpoint, i)
		}
		if err := stage.addOrder(o.OrderID, restingOrder{o.Side, o.PriceTicks, o.RemainingLots}); err != nil {
			return nil, fmt.Errorf("%w: order at index=%d: %v", ErrInvalidCheckpoint, i, err)
		}
		prev = o.OrderID
	}
	stage.commit()
	bestBid := bestTicks(p.bids, true)
	bestAsk := bestTicks(p.asks, false)
	if bestBid != 0 && bestAsk != 0 && bestBid >= bestAsk {
		return nil, fmt.Errorf("%w: crossed book", ErrInvalidCheckpoint)
	}
	p.sequence = cp.Sequence
	return p, nil
}

func bestTicks(levels map[int64]*big.Int, bids bool) int64 {
	var best int64
	for price, lots := range levels {
		if lots.Sign() <= 0 {
			continue
		}
		if best == 0 || (bids && price > best) || (!bids && price < best) {
			best = price
		}
	}
	return best
}
