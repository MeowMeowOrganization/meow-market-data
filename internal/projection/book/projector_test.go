package book

import (
	"errors"
	"math"
	"math/big"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	marketv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	marketdatav1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matchingv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	marketID uint32 = 7
	version  uint64 = 3
)

var (
	buy  = orderv1.OrderSide_ORDER_SIDE_BUY
	sell = orderv1.OrderSide_ORDER_SIDE_SELL
)

func definition() *marketdatav1.MarketProjectionDefinition {
	return &marketdatav1.MarketProjectionDefinition{
		SourceMarketId:    marketID,
		DefinitionVersion: version,
		Market: &marketv1.Market{
			MarketId: "BTC-USDT", PriceTickUnits: 25,
			QuantityStepBaseAtoms: 1_000,
		},
	}
}

func newBook(t *testing.T, depth uint32) *Projector {
	t.Helper()
	p, err := New(definition(), depth)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func place(id uint64, side orderv1.OrderSide, ticks, lots int64) *matchingv1.MatchingCommand {
	return &matchingv1.MatchingCommand{
		CommandId: "place-" + big.NewInt(int64(id)).String(),
		MarketId:  marketID,
		Command: &matchingv1.MatchingCommand_PlaceLimitOrder{
			PlaceLimitOrder: &matchingv1.PlaceLimitOrder{
				OrderId: id, Side: side, PriceTicks: ticks, QuantityLots: lots,
			},
		},
	}
}

func cancel(id uint64) *matchingv1.MatchingCommand {
	return &matchingv1.MatchingCommand{
		CommandId: "cancel-" + big.NewInt(int64(id)).String(),
		MarketId:  marketID,
		Command: &matchingv1.MatchingCommand_CancelOrder{
			CancelOrder: &matchingv1.CancelOrder{OrderId: id},
		},
	}
}

func committed(seq uint64, cmd *matchingv1.MatchingCommand, events ...*matchingv1.SequencedMatchingEvent) *matchingv1.CommittedMatchingResult {
	for ordinal, event := range events {
		event.EventId = &matchingv1.MatchingEventId{
			MarketId: marketID, MarketSequence: seq, EventOrdinal: uint64(ordinal),
		}
	}
	return &matchingv1.CommittedMatchingResult{
		Result:                  &matchingv1.MatchingResult{Command: cmd, MarketSequence: seq, Events: events},
		RecordedAt:              timestamppb.New(time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)),
		MarketDefinitionVersion: version,
	}
}

func execution(maker, taker uint64, takerSide orderv1.OrderSide, price, lots int64) *matchingv1.SequencedMatchingEvent {
	return &matchingv1.SequencedMatchingEvent{
		Event: &matchingv1.SequencedMatchingEvent_Exectuion{
			Exectuion: &matchingv1.Execution{
				MakerOrderId: maker, TakerOrderId: taker, TakerSide: takerSide,
				PriceTicks: price, QuantityLots: lots,
			},
		},
	}
}

func cancelled(id uint64, lots int64) *matchingv1.SequencedMatchingEvent {
	return &matchingv1.SequencedMatchingEvent{
		Event: &matchingv1.SequencedMatchingEvent_OrderCancelled{
			OrderCancelled: &matchingv1.OrderCancelled{OrderId: id, CancelledQuantityLots: lots},
		},
	}
}

func rejectedPlace(id uint64) *matchingv1.SequencedMatchingEvent {
	return &matchingv1.SequencedMatchingEvent{
		Event: &matchingv1.SequencedMatchingEvent_OrderRejected{
			OrderRejected: &matchingv1.OrderRejected{
				OrderId: id,
				Reason:  matchingv1.MatchingOrderRejectionReason_MATCHING_ORDER_REJECTION_REASON_DUPLICATE_ORDER_ID,
			},
		},
	}
}

func rejectedCancel(id uint64) *matchingv1.SequencedMatchingEvent {
	return &matchingv1.SequencedMatchingEvent{
		Event: &matchingv1.SequencedMatchingEvent_CancelRejected{
			CancelRejected: &matchingv1.CancelRejected{
				OrderId: id,
				Reason:  matchingv1.MatchingCancelRejectionReason_MATCHING_CANCEL_REJECTION_REASON_UNKNOWN_ORDER,
			},
		},
	}
}

func apply(t *testing.T, p *Projector, input *matchingv1.CommittedMatchingResult) *marketdatav1.BookDelta {
	t.Helper()
	delta, err := p.Apply(input)
	if err != nil {
		t.Fatal(err)
	}
	if delta == nil {
		t.Fatal("expected one new book delta")
	}
	return delta
}

func expectLevels(t *testing.T, got []*marketdatav1.BookLevel, want ...*marketdatav1.BookLevel) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v levels, want %v", got, want)
	}
	for i := range want {
		if got[i].GetPriceUnits() != want[i].GetPriceUnits() || got[i].GetBaseQuantityAtoms() != want[i].GetBaseQuantityAtoms() {
			t.Fatalf("level[%d] = %v; want %v", i, got[i], want[i])
		}
	}
}

func lvl(price uint64, quantity string) *marketdatav1.BookLevel {
	return &marketdatav1.BookLevel{PriceUnits: price, BaseQuantityAtoms: quantity}
}

func updates(got []*marketdatav1.BookLevelUpdate) [][2]string {
	out := make([][2]string, 0, len(got))
	for _, l := range got {
		out = append(out, [2]string{new(big.Int).SetUint64(l.GetPriceUnits()).String(), l.GetBaseQuantityAtoms()})
	}
	return out
}

func expectUpdates(t *testing.T, got []*marketdatav1.BookLevelUpdate, expected ...[2]string) {
	t.Helper()
	if len(got) == 0 && len(expected) == 0 {
		return
	}
	if !reflect.DeepEqual(updates(got), expected) {
		t.Fatalf("updates=%v; want=%v", updates(got), expected)
	}
}

func TestInitialStateAndZeroEventPlacement(t *testing.T) {
	p := newBook(t, 2)
	initial := p.Snapshot()
	if initial.GetMarketSequence() != 0 || initial.GetMarketId() != "BTC-USDT" || initial.GetDepth() != 2 || len(initial.GetBids()) != 0 {
		t.Fatalf("unexpected pristine snapshot: %+v", initial)
	}
	delta := apply(t, p, committed(1, place(10, buy, 400, 7)))
	if delta.GetPreviousMarketSequence() != 0 || delta.GetMarketSequence() != 1 || delta.GetMarketDefinitionVersion() != version || delta.GetDepth() != 2 {
		t.Fatalf("unexpected delta metadata: %+v", delta)
	}
	expectUpdates(t, delta.GetBidUpdates(), [2]string{"10000", "7000"})
	expectUpdates(t, delta.GetAskUpdates())
	expectLevels(t, p.Snapshot().GetBids(), lvl(10000, "7000"))
}

func TestPartialFillAndIncomingRemainderRests(t *testing.T) {
	p := newBook(t, 5)
	apply(t, p, committed(1, place(1, buy, 400, 3)))
	delta := apply(t, p, committed(2, place(2, sell, 399, 8), execution(1, 2, sell, 400, 3)))
	expectUpdates(t, delta.GetBidUpdates(), [2]string{"10000", "0"})
	expectUpdates(t, delta.GetAskUpdates(), [2]string{"9975", "5000"})
	snapshot := p.Snapshot()
	expectLevels(t, snapshot.GetBids())
	expectLevels(t, snapshot.GetAsks(), lvl(9975, "5000"))

	// Buy the remaining ask partially and ensure maker quantity is reduced.
	followup := apply(t, p, committed(3, place(3, buy, 401, 2), execution(2, 3, buy, 399, 2)))
	expectUpdates(t, followup.GetAskUpdates(), [2]string{"9975", "3000"})
	expectLevels(t, p.Snapshot().GetAsks(), lvl(9975, "3000"))
}

func TestMultipleMakersOneResult(t *testing.T) {
	p := newBook(t, 5)
	apply(t, p, committed(1, place(1, buy, 100, 2)))
	apply(t, p, committed(2, place(2, buy, 99, 3)))
	delta := apply(t, p, committed(3, place(3, sell, 98, 7),
		execution(1, 3, sell, 100, 2),
		execution(2, 3, sell, 99, 3),
	))
	expectUpdates(t, delta.GetBidUpdates(), [2]string{"2500", "0"}, [2]string{"2475", "0"})
	expectUpdates(t, delta.GetAskUpdates(), [2]string{"2450", "2000"})
	expectLevels(t, p.Snapshot().GetBids())
	expectLevels(t, p.Snapshot().GetAsks(), lvl(2450, "2000"))
}

func TestAggregationSortingAndDepthEviction(t *testing.T) {
	p := newBook(t, 2)
	apply(t, p, committed(1, place(10, buy, 100, 2)))
	delta := apply(t, p, committed(2, place(11, buy, 100, 3)))
	expectUpdates(t, delta.GetBidUpdates(), [2]string{"2500", "5000"})
	apply(t, p, committed(3, place(12, buy, 99, 8)))
	delta = apply(t, p, committed(4, place(13, buy, 101, 1)))
	expectUpdates(t, delta.GetBidUpdates(), [2]string{"2525", "1000"}, [2]string{"2475", "0"})
	expectLevels(t, p.Snapshot().GetBids(), lvl(2525, "1000"), lvl(2500, "5000"))

	// Removing the best price causes a previously evicted level to reappear.
	delta = apply(t, p, committed(5, cancel(13), cancelled(13, 1)))
	expectUpdates(t, delta.GetBidUpdates(), [2]string{"2525", "0"}, [2]string{"2475", "8000"})
	expectLevels(t, p.Snapshot().GetBids(), lvl(2500, "5000"), lvl(2475, "8000"))
}

func TestOffscreenLevelStillAdvancesSequence(t *testing.T) {
	p := newBook(t, 1)
	apply(t, p, committed(1, place(1, buy, 100, 1)))
	delta := apply(t, p, committed(2, place(2, buy, 50, 1)))
	expectUpdates(t, delta.GetBidUpdates())
	if p.Sequence() != 2 {
		t.Fatal("offscreen placement did not advance sequence")
	}
	delta = apply(t, p, committed(3, cancel(2), cancelled(2, 1)))
	expectUpdates(t, delta.GetBidUpdates())
}

func TestRejectionsHaveNoBookEffect(t *testing.T) {
	p := newBook(t, 3)
	apply(t, p, committed(1, place(1, buy, 400, 5)))
	original := p.Snapshot()
	// A duplicate active order ID is a normal matcher-level rejection.
	delta := apply(t, p, committed(2, place(1, buy, 400, 5), rejectedPlace(1)))
	expectUpdates(t, delta.GetBidUpdates())
	delta = apply(t, p, committed(3, cancel(999), rejectedCancel(999)))
	expectUpdates(t, delta.GetBidUpdates())
	current := p.Snapshot()
	if !proto.Equal(&marketdatav1.BookSnapshot{Bids: original.GetBids(), Asks: original.GetAsks()},
		&marketdatav1.BookSnapshot{Bids: current.GetBids(), Asks: current.GetAsks()}) {
		t.Fatal("rejection modified resting liquidity")
	}
}

func TestSuccessfulCancelRemovesExactRemainder(t *testing.T) {
	p := newBook(t, 2)
	apply(t, p, committed(1, place(1, sell, 101, 5)))
	delta := apply(t, p, committed(2, cancel(1), cancelled(1, 5)))
	expectUpdates(t, delta.GetAskUpdates(), [2]string{"2525", "0"})
	expectLevels(t, p.Snapshot().GetAsks())
}

func TestInvalidResultsRollbackAtomically(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*matchingv1.CommittedMatchingResult)
		want   error
	}{
		{"missing maker after earlier fill", func(c *matchingv1.CommittedMatchingResult) {
			c.Result.Events = []*matchingv1.SequencedMatchingEvent{
				execution(1, 3, sell, 100, 1), execution(999, 3, sell, 99, 1),
			}
			for i, ev := range c.Result.Events {
				ev.EventId = &matchingv1.MatchingEventId{MarketId: marketID, MarketSequence: 3, EventOrdinal: uint64(i)}
			}
		}, ErrInvariant},
		{"bad ordinal", func(c *matchingv1.CommittedMatchingResult) { c.Result.Events[0].EventId.EventOrdinal = 1 }, ErrInvalidResult},
		{"wrong event market", func(c *matchingv1.CommittedMatchingResult) { c.Result.Events[0].EventId.MarketId = 8 }, ErrInvalidResult},
		{"overfills incoming", func(c *matchingv1.CommittedMatchingResult) { c.Result.Events[0].GetExectuion().QuantityLots = 4 }, ErrInvariant},
		{"wrong maker price", func(c *matchingv1.CommittedMatchingResult) { c.Result.Events[0].GetExectuion().PriceTicks = 99 }, ErrInvariant},
		{"wrong taker side", func(c *matchingv1.CommittedMatchingResult) { c.Result.Events[0].GetExectuion().TakerSide = buy }, ErrInvariant},
		{"outside taker limit", func(c *matchingv1.CommittedMatchingResult) { c.Result.Command.GetPlaceLimitOrder().PriceTicks = 101 }, ErrInvariant},
		{"definition version", func(c *matchingv1.CommittedMatchingResult) { c.MarketDefinitionVersion++ }, ErrInvalidResult},
		{"missing timestamp", func(c *matchingv1.CommittedMatchingResult) { c.RecordedAt = nil }, ErrInvalidResult},
		{"invalid timestamp", func(c *matchingv1.CommittedMatchingResult) { c.RecordedAt.Seconds = 1 << 62 }, ErrInvalidResult},
		{"missing command id", func(c *matchingv1.CommittedMatchingResult) { c.Result.Command.CommandId = "" }, ErrInvalidResult},
		{"mismatched command market", func(c *matchingv1.CommittedMatchingResult) { c.Result.Command.MarketId = 8 }, ErrInvalidResult},
		{"mixed rejection and execution", func(c *matchingv1.CommittedMatchingResult) {
			c.Result.Events = append(c.Result.Events, rejectedPlace(3))
			c.Result.Events[1].EventId = &matchingv1.MatchingEventId{MarketId: marketID, MarketSequence: 3, EventOrdinal: 1}
		}, ErrInvalidResult},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newBook(t, 3)
			apply(t, p, committed(1, place(1, buy, 100, 3)))
			apply(t, p, committed(2, place(2, buy, 99, 3)))
			before := p.Snapshot()
			valid := committed(3, place(3, sell, 98, 2), execution(1, 3, sell, 100, 2))
			tc.mutate(valid)
			if _, err := p.Apply(valid); !errors.Is(err, tc.want) {
				t.Fatalf("got error %v; want %v", err, tc.want)
			}
			if !proto.Equal(before, p.Snapshot()) {
				t.Fatalf("failed projection mutated book\nbefore=%v\nafter=%v", before, p.Snapshot())
			}
			// A subsequent valid result at the SAME sequence must still apply.
			apply(t, p, committed(3, place(4, sell, 98, 2), execution(1, 4, sell, 100, 2)))
		})
	}
}

func TestInvalidCancelRollback(t *testing.T) {
	p := newBook(t, 2)
	apply(t, p, committed(1, place(1, sell, 100, 4)))
	before := p.Snapshot()
	if _, err := p.Apply(committed(2, cancel(1), cancelled(1, 3))); !errors.Is(err, ErrInvariant) {
		t.Fatalf("bad remaining quantity accepted: %v", err)
	}
	if !proto.Equal(before, p.Snapshot()) {
		t.Fatal("invalid cancellation mutated book")
	}
	apply(t, p, committed(2, cancel(1), cancelled(1, 4)))
}

func TestSequenceGapDuplicatesAndConflicts(t *testing.T) {
	p := newBook(t, 1)
	first := committed(1, place(1, buy, 400, 2))
	apply(t, p, first)
	if delta, err := p.Apply(proto.Clone(first).(*matchingv1.CommittedMatchingResult)); err != nil || delta != nil {
		t.Fatalf("replay should be no-op: delta=%v err=%v", delta, err)
	}
	conflicting := proto.Clone(first).(*matchingv1.CommittedMatchingResult)
	conflicting.RecordedAt.Seconds++
	if _, err := p.Apply(conflicting); !errors.Is(err, ErrConflictingReplay) {
		t.Fatalf("expected conflict: %v", err)
	}
	if _, err := p.Apply(committed(3, place(3, buy, 400, 1))); !errors.Is(err, ErrSequenceGap) {
		t.Fatalf("expected gap: %v", err)
	}
	apply(t, p, committed(2, place(2, buy, 400, 1)))
	if _, err := p.Apply(first); !errors.Is(err, ErrStaleResult) {
		t.Fatalf("expected stale: %v", err)
	}
	expectLevels(t, p.Snapshot().GetBids(), lvl(10000, "3000"))
}

func TestExactBigIntegerAggregationAndPriceOverflow(t *testing.T) {
	def := definition()
	def.Market.QuantityStepBaseAtoms = math.MaxUint64
	def.Market.PriceTickUnits = 1
	p, err := New(def, 1)
	if err != nil {
		t.Fatal(err)
	}
	apply(t, p, committed(1, place(1, buy, 1, math.MaxInt64)))
	apply(t, p, committed(2, place(2, buy, 1, math.MaxInt64)))
	expected := new(big.Int).Mul(new(big.Int).SetInt64(math.MaxInt64), new(big.Int).SetUint64(math.MaxUint64))
	expected.Mul(expected, big.NewInt(2))
	expectLevels(t, p.Snapshot().GetBids(), lvl(1, expected.String()))

	overflowDef := definition()
	overflowDef.Market.PriceTickUnits = math.MaxUint64
	overflowBook, err := New(overflowDef, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := overflowBook.Apply(committed(1, place(1, buy, 2, 1))); !errors.Is(err, ErrInvariant) {
		t.Fatalf("price overflow unexpectedly accepted: %v", err)
	}
	if overflowBook.Sequence() != 0 {
		t.Fatal("overflow advanced sequence")
	}
}

func TestDefinitionIsolationSnapshotIsolationAndValidation(t *testing.T) {
	d := definition()
	p, err := New(d, 1)
	if err != nil {
		t.Fatal(err)
	}
	d.Market.PriceTickUnits = 1000
	d.Market.MarketId = "TAMPER"
	apply(t, p, committed(1, place(1, buy, 10, 5)))
	snap := p.Snapshot()
	snap.Bids[0].BaseQuantityAtoms = "-123"
	snap.MarketId = "BAD"
	expectLevels(t, p.Snapshot().GetBids(), lvl(250, "5000"))
	if p.Snapshot().GetMarketId() != "BTC-USDT" {
		t.Fatal("definition alias retained")
	}

	badDefs := []struct {
		d     *marketdatav1.MarketProjectionDefinition
		depth uint32
	}{
		{nil, 1}, {&marketdatav1.MarketProjectionDefinition{}, 1},
		{definition(), 0},
	}
	for _, bad := range badDefs {
		if _, err := New(bad.d, bad.depth); !errors.Is(err, ErrInvalidDefinition) {
			t.Fatalf("expected invalid definition: %v", err)
		}
	}
}

func TestConcurrentReadersAndSequentialWriter(t *testing.T) {
	p := newBook(t, 2)
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < n; j++ {
				snapshot := p.Snapshot()
				if snapshot.GetMarketId() != "BTC-USDT" || snapshot.GetDepth() != 2 {
					t.Error("incoherent snapshot")
				}
				_ = p.Sequence()
			}
		}()
	}
	for i := uint64(1); i <= n; i++ {
		apply(t, p, committed(i, place(i, buy, int64(100+i), 1)))
	}
	wg.Wait()
	if p.Sequence() != n {
		t.Fatal("sequence was not fully advanced")
	}
	expectLevels(t, p.Snapshot().GetBids(), lvl((100+n)*25, "1000"), lvl((99+n)*25, "1000"))
}

// Book outputs are sufficient to update a top-N client without having the
// projector's private resting-order index. This verifies depth eviction too.
func TestDeltasReconstructSnapshots(t *testing.T) {
	p := newBook(t, 2)
	client := p.Snapshot()
	inputs := []*matchingv1.CommittedMatchingResult{
		committed(1, place(1, buy, 100, 2)),
		committed(2, place(2, buy, 99, 4)),
		committed(3, place(3, buy, 101, 1)),
		committed(4, cancel(3), cancelled(3, 1)),
		committed(5, place(4, sell, 100, 2), execution(1, 4, sell, 100, 2)),
		committed(6, cancel(2), cancelled(2, 4)),
	}
	for _, in := range inputs {
		delta := apply(t, p, in)
		if client.GetMarketSequence() != delta.GetPreviousMarketSequence() {
			t.Fatal("client stream sequence discontinuity")
		}
		merge := func(levels []*marketdatav1.BookLevel, changes []*marketdatav1.BookLevelUpdate, bid bool) []*marketdatav1.BookLevel {
			values := map[uint64]string{}
			for _, l := range levels {
				values[l.GetPriceUnits()] = l.GetBaseQuantityAtoms()
			}
			for _, change := range changes {
				if change.GetBaseQuantityAtoms() == "0" {
					delete(values, change.GetPriceUnits())
				} else {
					values[change.GetPriceUnits()] = change.GetBaseQuantityAtoms()
				}
			}
			prices := make([]uint64, 0, len(values))
			for price := range values {
				prices = append(prices, price)
			}
			sort.Slice(prices, func(i, j int) bool {
				if bid {
					return prices[i] > prices[j]
				}
				return prices[i] < prices[j]
			})
			if len(prices) > 2 {
				prices = prices[:2]
			}
			out := make([]*marketdatav1.BookLevel, 0, len(prices))
			for _, price := range prices {
				out = append(out, lvl(price, values[price]))
			}
			return out
		}
		client.Bids = merge(client.GetBids(), delta.GetBidUpdates(), true)
		client.Asks = merge(client.GetAsks(), delta.GetAskUpdates(), false)
		client.MarketSequence = delta.GetMarketSequence()
		if !proto.Equal(client, p.Snapshot()) {
			t.Fatalf("snapshot/delta divergence at sequence %d: client=%v server=%v", client.MarketSequence, client, p.Snapshot())
		}
	}
}

func TestCrossedBookRejectedAtomically(t *testing.T) {
	p := newBook(t, 2)
	apply(t, p, committed(1, place(1, sell, 100, 2)))
	before := p.Snapshot()
	// A correctly functioning matcher would execute this buy at the ask.
	if _, err := p.Apply(committed(2, place(2, buy, 101, 1))); !errors.Is(err, ErrInvariant) {
		t.Fatalf("crossed book unexpectedly accepted: %v", err)
	}
	if !proto.Equal(before, p.Snapshot()) {
		t.Fatal("crossed result changed book")
	}
	apply(t, p, committed(2, place(3, buy, 100, 1), execution(1, 3, buy, 100, 1)))
}

func TestUnknownRejectionCodeAndEmptyCancellationRejected(t *testing.T) {
	p := newBook(t, 2)
	bad := committed(1, place(1, buy, 100, 1), rejectedPlace(1))
	bad.Result.Events[0].GetOrderRejected().Reason = 128
	if _, err := p.Apply(bad); !errors.Is(err, ErrInvalidResult) {
		t.Fatalf("unknown rejection accepted: %v", err)
	}
	bad2 := committed(1, cancel(1), rejectedCancel(1))
	bad2.Result.Events[0].GetEvent().(*matchingv1.SequencedMatchingEvent_CancelRejected).CancelRejected.Reason = 123
	if _, err := p.Apply(bad2); !errors.Is(err, ErrInvalidResult) {
		t.Fatalf("unknown cancel rejection accepted: %v", err)
	}
	if p.Sequence() != 0 {
		t.Fatal("rejection validation advanced sequence")
	}
}
