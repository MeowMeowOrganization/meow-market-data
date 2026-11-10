package book

import (
	"fmt"
	"math/big"

	matchingv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
)

// stage records only the orders and price levels touched by one command.
// Until commit() succeeds, Projector's maps remain entirely unchanged.
// Nil order writes mean delete; zero aggregate writes mean remove level.
type stage struct {
	p            *Projector
	orderChanges map[uint64]*restingOrder
	bidChanges   map[int64]*big.Int
	askChanges   map[int64]*big.Int
}

func newStage(p *Projector) *stage {
	return &stage{
		p:            p,
		orderChanges: make(map[uint64]*restingOrder),
		bidChanges:   make(map[int64]*big.Int),
		askChanges:   make(map[int64]*big.Int),
	}
}

func (s *stage) order(id uint64) (restingOrder, bool) {
	if o, seen := s.orderChanges[id]; seen {
		if o == nil {
			return restingOrder{}, false
		}
		return *o, true
	}
	o, ok := s.p.orders[id]
	return o, ok
}

func (s *stage) addOrder(id uint64, order restingOrder) error {
	if _, exists := s.order(id); exists {
		return fmt.Errorf("%w: duplicate resting id=%d", ErrInvariant, id)
	}
	if err := s.adjustLevel(order.side, order.priceTicks, big.NewInt(order.remainingLots)); err != nil {
		return err
	}
	copy := order
	s.orderChanges[id] = &copy
	return nil
}

func (s *stage) removeOrder(id uint64, order restingOrder) error {
	if err := s.adjustLevel(order.side, order.priceTicks, new(big.Int).Neg(big.NewInt(order.remainingLots))); err != nil {
		return err
	}
	s.orderChanges[id] = nil
	return nil
}

func (s *stage) fillMaker(ex *matchingv1.Execution, takerSide orderv1.OrderSide) error {
	if ex.GetMakerOrderId() == ex.GetTakerOrderId() {
		return fmt.Errorf("%w: maker equals taker", ErrInvariant)
	}
	maker, found := s.order(ex.GetMakerOrderId())
	if !found {
		return fmt.Errorf("%w: missing maker id=%d", ErrInvariant, ex.GetMakerOrderId())
	}
	if maker.side == takerSide || maker.priceTicks != ex.GetPriceTicks() || ex.GetQuantityLots() > maker.remainingLots {
		return fmt.Errorf("%w: maker side/price/quantity does not match execution", ErrInvariant)
	}
	if err := s.adjustLevel(maker.side, maker.priceTicks, new(big.Int).Neg(big.NewInt(ex.GetQuantityLots()))); err != nil {
		return err
	}
	maker.remainingLots -= ex.GetQuantityLots()
	if maker.remainingLots == 0 {
		s.orderChanges[ex.GetMakerOrderId()] = nil
	} else {
		copy := maker
		s.orderChanges[ex.GetMakerOrderId()] = &copy
	}
	return nil
}

func (s *stage) adjustLevel(side orderv1.OrderSide, ticks int64, delta *big.Int) error {
	var changes, existing map[int64]*big.Int
	switch side {
	case orderv1.OrderSide_ORDER_SIDE_BUY:
		changes, existing = s.bidChanges, s.p.bids
	case orderv1.OrderSide_ORDER_SIDE_SELL:
		changes, existing = s.askChanges, s.p.asks
	default:
		return fmt.Errorf("%w: unsupported resting side", ErrInvalidResult)
	}
	current, touched := changes[ticks]
	if !touched {
		current = existing[ticks]
	}
	updated := new(big.Int)
	if current != nil {
		updated.Set(current)
	}
	updated.Add(updated, delta)
	if updated.Sign() < 0 {
		return fmt.Errorf("%w: negative aggregate at ticks=%d", ErrInvariant, ticks)
	}
	changes[ticks] = updated
	return nil
}

func (s *stage) commit() {
	for id, o := range s.orderChanges {
		if o == nil {
			delete(s.p.orders, id)
		} else {
			s.p.orders[id] = *o
		}
	}
	for ticks, count := range s.bidChanges {
		if count.Sign() == 0 {
			delete(s.p.bids, ticks)
		} else {
			s.p.bids[ticks] = count
		}
	}
	for ticks, count := range s.askChanges {
		if count.Sign() == 0 {
			delete(s.p.asks, ticks)
		} else {
			s.p.asks[ticks] = count
		}
	}
}
