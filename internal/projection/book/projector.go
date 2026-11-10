// Package book implements a deterministic, dependency-free Level-2 projection
// of committed matching results. The only non-stdlib imports are protobuf
// contracts and the protobuf runtime; it has no infrastructure dependencies.
package book

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"

	marketdatav1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matchingv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidDefinition = errors.New("book: invalid market projection definition")
	ErrInvalidResult     = errors.New("book: invalid committed matching result")
	ErrSequenceGap       = errors.New("book: market sequence gap")
	ErrStaleResult       = errors.New("book: result older than last committed sequence")
	ErrConflictingReplay = errors.New("book: conflicting replay at current sequence")
	ErrInvariant         = errors.New("book: matching/book invariant violation")
)

type restingOrder struct {
	side          orderv1.OrderSide
	priceTicks    int64
	remainingLots int64
}

// Projector is the stateful, in-memory projection of exactly one numeric market
// and one immutable market-definition version. Apply and Snapshot are safe for
// concurrent calls; Apply serializes complete matching results.
//
// No arbitrary Kafka offset is accepted as book sequence. The first Apply on a
// newly created projector MUST be market sequence 1. Restoring a later snapshot
// is an explicit future milestone, not inferred from Redis state.
//
// Identical redelivery of the most recently applied result is a no-op. Older
// sequences are rejected (not silently acknowledged) because this in-memory
// implementation does not retain an unbounded provenance log.
//
// One projector per market/definition; changing version requires a coordinated
// transition/recovery rather than modifying a live Projector.
type Projector struct {
	mu sync.Mutex

	symbol            string
	sourceMarketID    uint32
	definitionVersion uint64
	tickUnits         uint64
	lotAtoms          uint64
	depth             uint32

	sequence uint64
	orders   map[uint64]restingOrder
	bids     map[int64]*big.Int // aggregated LOTS by price tick (never float)
	asks     map[int64]*big.Int

	haveLastDigest bool
	lastDigest     [sha256.Size]byte
}

// New validates and copies the immutable subset of a market projection
// definition used by the book. depth is the fixed public L2 depth; changing
// it requires creating a separate projector/output stream.
func New(def *marketdatav1.MarketProjectionDefinition, depth uint32) (*Projector, error) {
	if def == nil || def.GetMarket() == nil ||
		def.GetMarket().GetMarketId() == "" || def.GetDefinitionVersion() == 0 ||
		def.GetMarket().GetPriceTickUnits() == 0 ||
		def.GetMarket().GetQuantityStepBaseAtoms() == 0 || depth == 0 {
		return nil, fmt.Errorf("%w: market, symbol, definition version, tick units, lot atoms and depth must be nonzero", ErrInvalidDefinition)
	}
	market := def.GetMarket()
	return &Projector{
		symbol:            market.GetMarketId(),
		sourceMarketID:    def.GetSourceMarketId(),
		definitionVersion: def.GetDefinitionVersion(),
		tickUnits:         market.GetPriceTickUnits(),
		lotAtoms:          market.GetQuantityStepBaseAtoms(),
		depth:             depth,
		orders:            make(map[uint64]restingOrder),
		bids:              make(map[int64]*big.Int),
		asks:              make(map[int64]*big.Int),
	}, nil
}

// Sequence reports the last fully applied committed market sequence.
func (p *Projector) Sequence() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sequence
}

// Snapshot returns a new protobuf with no aliases to internal mutable state.
// A pristine projector returns a snapshot at sequence 0 with empty levels.
func (p *Projector) Snapshot() *marketdatav1.BookSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked()
}

// Apply atomically projects one complete CommittedMatchingResult and returns
// EXACTLY one BookDelta for a newly applied sequence, including no-op results.
// For an identical retry of the most recent result, returns (nil, nil).
// On any error, no orders, levels, sequence, or replay digest are modified.
//
// Callers must publish each returned delta before marking their source record
// fully processed. Durability, source offsets, and Redis writes are deliberately
// outside this pure domain layer.
func (p *Projector) Apply(input *matchingv1.CommittedMatchingResult) (*marketdatav1.BookDelta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.validateEnvelope(input); err != nil {
		return nil, err
	}
	result := input.GetResult()
	sequence := result.GetMarketSequence()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal input: %v", ErrInvalidResult, err)
	}
	digest := sha256.Sum256(encoded)

	if sequence <= p.sequence {
		if sequence == p.sequence {
			if p.haveLastDigest && digest == p.lastDigest {
				return nil, nil
			}
			return nil, fmt.Errorf("%w: market=%d sequence=%d", ErrConflictingReplay, p.sourceMarketID, sequence)
		}
		return nil, fmt.Errorf("%w: got=%d applied=%d", ErrStaleResult, sequence, p.sequence)
	}
	if sequence-p.sequence != 1 {
		return nil, fmt.Errorf("%w: expected=%d got=%d", ErrSequenceGap, p.sequence+1, sequence)
	}

	beforeBids := visibleLevels(p.bids, nil, p.depth, true, p.tickUnits, p.lotAtoms)
	beforeAsks := visibleLevels(p.asks, nil, p.depth, false, p.tickUnits, p.lotAtoms)
	stage := newStage(p)
	if err := p.applyToStage(stage, result); err != nil {
		return nil, err
	}
	afterBids := visibleLevels(p.bids, stage.bidChanges, p.depth, true, p.tickUnits, p.lotAtoms)
	afterAsks := visibleLevels(p.asks, stage.askChanges, p.depth, false, p.tickUnits, p.lotAtoms)
	// A properly committed price-time matching book must never be crossed.
	// This assertion validates the result without reimplementing matching.
	if len(afterBids) != 0 && len(afterAsks) != 0 && afterBids[0].price >= afterAsks[0].price {
		return nil, fmt.Errorf("%w: crossed book at sequence=%d", ErrInvariant, sequence)
	}

	delta := &marketdatav1.BookDelta{
		MarketId:                p.symbol,
		SourceMarketId:          p.sourceMarketID,
		PreviousMarketSequence:  p.sequence,
		MarketSequence:          sequence,
		MarketDefinitionVersion: p.definitionVersion,
		Depth:                   p.depth,
		BidUpdates:              diffVisible(beforeBids, afterBids, true),
		AskUpdates:              diffVisible(beforeAsks, afterAsks, false),
	}
	stage.commit()
	p.sequence = sequence
	p.haveLastDigest = true
	p.lastDigest = digest
	return delta, nil
}

func (p *Projector) validateEnvelope(input *matchingv1.CommittedMatchingResult) error {
	if input == nil || input.GetResult() == nil || input.GetResult().GetCommand() == nil {
		return fmt.Errorf("%w: missing envelope, result or command", ErrInvalidResult)
	}
	if input.GetRecordedAt() == nil {
		return fmt.Errorf("%w: missing durable recorded_at", ErrInvalidResult)
	}
	if err := input.GetRecordedAt().CheckValid(); err != nil {
		return fmt.Errorf("%w: recorded_at: %v", ErrInvalidResult, err)
	}
	if input.GetMarketDefinitionVersion() != p.definitionVersion {
		return fmt.Errorf("%w: definition version got=%d want=%d", ErrInvalidResult, input.GetMarketDefinitionVersion(), p.definitionVersion)
	}
	result := input.GetResult()
	cmd := result.GetCommand()
	if cmd.GetMarketId() != p.sourceMarketID || cmd.GetCommandId() == "" || result.GetMarketSequence() == 0 {
		return fmt.Errorf("%w: market mismatch, empty command_id or zero sequence", ErrInvalidResult)
	}
	if h := result.GetStateHashAfter(); h != nil && (h.GetEncodingVersion() == 0 || len(h.GetSha256()) != sha256.Size) {
		return fmt.Errorf("%w: malformed matching state hash", ErrInvalidResult)
	}
	for i, event := range result.GetEvents() {
		if event == nil || event.GetEventId() == nil || event.GetEvent() == nil {
			return fmt.Errorf("%w: event %d missing identity or variant", ErrInvalidResult, i)
		}
		eid := event.GetEventId()
		if eid.GetMarketId() != p.sourceMarketID || eid.GetMarketSequence() != result.GetMarketSequence() || eid.GetEventOrdinal() != uint64(i) {
			return fmt.Errorf("%w: invalid event identity at ordinal %d", ErrInvalidResult, i)
		}
	}
	return nil
}

func (p *Projector) applyToStage(t *stage, result *matchingv1.MatchingResult) error {
	cmd := result.GetCommand()
	switch c := cmd.GetCommand().(type) {
	case *matchingv1.MatchingCommand_PlaceLimitOrder:
		if c.PlaceLimitOrder == nil {
			return fmt.Errorf("%w: nil placement", ErrInvalidResult)
		}
		return p.applyPlacement(t, c.PlaceLimitOrder, result.GetEvents())
	case *matchingv1.MatchingCommand_CancelOrder:
		if c.CancelOrder == nil {
			return fmt.Errorf("%w: nil cancellation", ErrInvalidResult)
		}
		return p.applyCancellation(t, c.CancelOrder, result.GetEvents())
	default:
		return fmt.Errorf("%w: unknown command variant", ErrInvalidResult)
	}
}

func (p *Projector) applyPlacement(t *stage, place *matchingv1.PlaceLimitOrder, events []*matchingv1.SequencedMatchingEvent) error {
	if place.GetPriceTicks() <= 0 || place.GetQuantityLots() <= 0 || !validSide(place.GetSide()) {
		return fmt.Errorf("%w: nonpositive placement tick/lot or invalid side", ErrInvalidResult)
	}
	if !priceFits(place.GetPriceTicks(), p.tickUnits) {
		return fmt.Errorf("%w: price conversion to uint64 overflows", ErrInvariant)
	}
	// A duplicate active order may legitimately produce OrderRejected. Validate
	// that outcome BEFORE requiring the placement ID to be absent.
	if len(events) == 1 {
		if rejected := events[0].GetOrderRejected(); rejected != nil {
			if rejected.GetOrderId() != place.GetOrderId() ||
				!validOrderRejection(rejected.GetReason()) {
				return fmt.Errorf("%w: malformed placement rejection", ErrInvalidResult)
			}
			return nil
		}
	}
	if _, exists := t.order(place.GetOrderId()); exists {
		return fmt.Errorf("%w: accepted placement duplicates active order=%d", ErrInvariant, place.GetOrderId())
	}

	remaining := place.GetQuantityLots()
	for i, event := range events {
		switch e := event.GetEvent().(type) {
		case *matchingv1.SequencedMatchingEvent_Exectuion: // typo is in the existing wire/source contract
			if e.Exectuion == nil {
				return fmt.Errorf("%w: nil execution at %d", ErrInvalidResult, i)
			}
			ex := e.Exectuion
			if ex.GetTakerOrderId() != place.GetOrderId() || ex.GetTakerSide() != place.GetSide() ||
				ex.GetQuantityLots() <= 0 || ex.GetPriceTicks() <= 0 || ex.GetQuantityLots() > remaining {
				return fmt.Errorf("%w: inconsistent incoming execution at %d", ErrInvariant, i)
			}
			if (place.GetSide() == orderv1.OrderSide_ORDER_SIDE_BUY && ex.GetPriceTicks() > place.GetPriceTicks()) ||
				(place.GetSide() == orderv1.OrderSide_ORDER_SIDE_SELL && ex.GetPriceTicks() < place.GetPriceTicks()) {
				return fmt.Errorf("%w: execution outside taker limit at %d", ErrInvariant, i)
			}
			if err := t.fillMaker(ex, place.GetSide()); err != nil {
				return fmt.Errorf("execution[%d]: %w", i, err)
			}
			remaining -= ex.GetQuantityLots()
		case *matchingv1.SequencedMatchingEvent_OrderRejected:
			return fmt.Errorf("%w: placement rejection mixed with executions", ErrInvalidResult)
		default:
			return fmt.Errorf("%w: unexpected event for placement at %d", ErrInvalidResult, i)
		}
	}
	if remaining > 0 {
		if err := t.addOrder(place.GetOrderId(), restingOrder{place.GetSide(), place.GetPriceTicks(), remaining}); err != nil {
			return err
		}
	}
	return nil
}

func (p *Projector) applyCancellation(t *stage, cmd *matchingv1.CancelOrder, events []*matchingv1.SequencedMatchingEvent) error {
	if len(events) != 1 {
		return fmt.Errorf("%w: cancellation requires exactly one terminal outcome", ErrInvalidResult)
	}
	switch ev := events[0].GetEvent().(type) {
	case *matchingv1.SequencedMatchingEvent_OrderCancelled:
		if ev.OrderCancelled == nil || ev.OrderCancelled.GetOrderId() != cmd.GetOrderId() || ev.OrderCancelled.GetCancelledQuantityLots() <= 0 {
			return fmt.Errorf("%w: malformed order cancellation", ErrInvalidResult)
		}
		current, found := t.order(cmd.GetOrderId())
		if !found || current.remainingLots != ev.OrderCancelled.GetCancelledQuantityLots() {
			return fmt.Errorf("%w: cancellation remainder differs from resting book", ErrInvariant)
		}
		return t.removeOrder(cmd.GetOrderId(), current)
	case *matchingv1.SequencedMatchingEvent_CancelRejected:
		if ev.CancelRejected == nil || ev.CancelRejected.GetOrderId() != cmd.GetOrderId() ||
			!validCancelRejection(ev.CancelRejected.GetReason()) {
			return fmt.Errorf("%w: malformed cancel rejection", ErrInvalidResult)
		}
		if _, found := t.order(cmd.GetOrderId()); found {
			return fmt.Errorf("%w: matcher rejected cancellation of active order", ErrInvariant)
		}
		return nil
	default:
		return fmt.Errorf("%w: unexpected cancellation outcome", ErrInvalidResult)
	}
}

func validSide(side orderv1.OrderSide) bool {
	return side == orderv1.OrderSide_ORDER_SIDE_BUY || side == orderv1.OrderSide_ORDER_SIDE_SELL
}

func validOrderRejection(r matchingv1.MatchingOrderRejectionReason) bool {
	switch r {
	case matchingv1.MatchingOrderRejectionReason_MATCHING_ORDER_REJECTION_REASON_DUPLICATE_ORDER_ID,
		matchingv1.MatchingOrderRejectionReason_MATCHING_ORDER_REJECTION_REASON_RESTING_AGGREGATE_QUANTITY_OUT_OF_RANGE:
		return true
	default:
		return false
	}
}

func validCancelRejection(r matchingv1.MatchingCancelRejectionReason) bool {
	switch r {
	case matchingv1.MatchingCancelRejectionReason_MATCHING_CANCEL_REJECTION_REASON_UNKNOWN_ORDER,
		matchingv1.MatchingCancelRejectionReason_MATCHING_CANCEL_REJECTION_REASON_ALREADY_FILLED,
		matchingv1.MatchingCancelRejectionReason_MATCHING_CANCEL_REJECTION_REASON_ALREADY_CANCELLED,
		matchingv1.MatchingCancelRejectionReason_MATCHING_CANCEL_REJECTION_REASON_ALREADY_REJECTED:
		return true
	default:
		return false
	}
}

func priceFits(ticks int64, units uint64) bool {
	if ticks <= 0 || units == 0 {
		return false
	}
	return uint64(ticks) <= math.MaxUint64/units
}
