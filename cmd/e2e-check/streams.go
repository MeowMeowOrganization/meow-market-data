package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	order "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// All three subscriptions run against the SAME real service and topic. Each
// RPC has its own bounded lifetime: the unary client's 8-second HTTP timeout
// must NOT be reused for a long-lived Connect server stream.
const streamingE2ETimeout = 45 * time.Second

func receiveBook(s *connect.ServerStreamForClient[api.StreamOrderBookResponse]) (*api.StreamOrderBookResponse, error) {
	if !s.Receive() {
		if err := s.Err(); err != nil {
			return nil, fmt.Errorf("StreamOrderBook terminated: %w", err)
		}
		return nil, fmt.Errorf("StreamOrderBook ended without an error")
	}
	return s.Msg(), nil
}

func receiveTrade(s *connect.ServerStreamForClient[api.StreamTradesResponse]) (*api.StreamTradesResponse, error) {
	if !s.Receive() {
		if err := s.Err(); err != nil {
			return nil, fmt.Errorf("StreamTrades terminated: %w", err)
		}
		return nil, fmt.Errorf("StreamTrades ended without an error")
	}
	return s.Msg(), nil
}

func receiveTicker(s *connect.ServerStreamForClient[api.StreamTickerResponse]) (*api.StreamTickerResponse, error) {
	if !s.Receive() {
		if err := s.Err(); err != nil {
			return nil, fmt.Errorf("StreamTicker terminated: %w", err)
		}
		return nil, fmt.Errorf("StreamTicker ended without an error")
	}
	return s.Msg(), nil
}

// bookView applies absolute set-to L2 deltas, rather than only checking the
// sequence numbers. It catches a stream that is gapless but materially wrong.
type bookView struct {
	sequence uint64
	bids     map[uint64]string
	asks     map[uint64]string
}

func levelsView(levels []*md.BookLevel) (map[uint64]string, error) {
	out := make(map[uint64]string, len(levels))
	for _, level := range levels {
		if level == nil || level.GetPriceUnits() == 0 || level.GetBaseQuantityAtoms() == "" || level.GetBaseQuantityAtoms() == "0" {
			return nil, fmt.Errorf("invalid nonzero book level: %v", level)
		}
		if _, ok := out[level.GetPriceUnits()]; ok {
			return nil, fmt.Errorf("duplicate book price %d", level.GetPriceUnits())
		}
		out[level.GetPriceUnits()] = level.GetBaseQuantityAtoms()
	}
	return out, nil
}

func viewFromSnapshot(snapshot *md.BookSnapshot, sequence uint64) (*bookView, error) {
	if snapshot == nil || snapshot.GetMarketId() != marketSymbol || snapshot.GetSourceMarketId() != sourceID ||
		snapshot.GetMarketDefinitionVersion() != definition || snapshot.GetDepth() != 20 || snapshot.GetMarketSequence() != sequence {
		return nil, fmt.Errorf("invalid Connect book snapshot at seq=%d: %v", sequence, snapshot)
	}
	bids, err := levelsView(snapshot.GetBids())
	if err != nil {
		return nil, err
	}
	asks, err := levelsView(snapshot.GetAsks())
	if err != nil {
		return nil, err
	}
	v := &bookView{sequence: sequence, bids: bids, asks: asks}
	return v, v.expectFixture(sequence)
}

func (v *bookView) apply(delta *md.BookDelta) error {
	if delta == nil || delta.GetMarketId() != marketSymbol || delta.GetSourceMarketId() != sourceID ||
		delta.GetMarketDefinitionVersion() != definition || delta.GetDepth() != 20 ||
		delta.GetPreviousMarketSequence() != v.sequence || delta.GetMarketSequence() != v.sequence+1 {
		return fmt.Errorf("book delta not contiguous: previous=%d delta=%v", v.sequence, delta)
	}
	applySide := func(levels map[uint64]string, updates []*md.BookLevelUpdate) error {
		seen := make(map[uint64]bool, len(updates))
		for _, u := range updates {
			if u == nil || u.GetPriceUnits() == 0 || u.GetBaseQuantityAtoms() == "" || seen[u.GetPriceUnits()] {
				return fmt.Errorf("invalid/duplicate absolute L2 update: %v", u)
			}
			seen[u.GetPriceUnits()] = true
			if u.GetBaseQuantityAtoms() == "0" {
				delete(levels, u.GetPriceUnits())
			} else {
				levels[u.GetPriceUnits()] = u.GetBaseQuantityAtoms()
			}
		}
		return nil
	}
	if err := applySide(v.bids, delta.GetBidUpdates()); err != nil {
		return err
	}
	if err := applySide(v.asks, delta.GetAskUpdates()); err != nil {
		return err
	}
	v.sequence = delta.GetMarketSequence()
	return v.expectFixture(v.sequence)
}

func (v *bookView) expectFixture(seq uint64) error {
	var asks map[uint64]string
	switch seq {
	case 3:
		asks = map[uint64]string{price: "2000"}
	case 4:
		asks = map[uint64]string{price: "2000", price + 25: "2000"}
	case 5:
		asks = map[uint64]string{}
	default:
		return fmt.Errorf("unexpected fixture sequence %d", seq)
	}
	if len(v.bids) != 0 || !reflect.DeepEqual(v.asks, asks) {
		return fmt.Errorf("Connect L2 at seq=%d: bids=%v asks=%v want asks=%v", seq, v.bids, v.asks, asks)
	}
	return nil
}

func (c *checker) openBookStream(ctx context.Context, seq uint64) (*connect.ServerStreamForClient[api.StreamOrderBookResponse], *bookView, error) {
	s, err := c.streamRPC.StreamOrderBook(ctx, connect.NewRequest(&api.StreamOrderBookRequest{MarketId: marketSymbol, Depth: 20}))
	if err != nil {
		return nil, nil, err
	}
	first, err := receiveBook(s)
	if err == nil && first.GetSnapshot() == nil {
		err = fmt.Errorf("first book event must be snapshot, got %v", first)
	}
	var view *bookView
	if err == nil {
		view, err = viewFromSnapshot(first.GetSnapshot(), seq)
	}
	if err != nil {
		_ = s.Close()
		return nil, nil, err
	}
	return s, view, nil
}

func expectBookNext(s *connect.ServerStreamForClient[api.StreamOrderBookResponse], v *bookView, want uint64) error {
	for {
		e, err := receiveBook(s)
		if err != nil {
			return err
		}
		if heartbeat := e.GetHeartbeat(); heartbeat != nil {
			if heartbeat.GetLastDeliveredMarketSequence() != v.sequence {
				return fmt.Errorf("book heartbeat cursor %d want %d", heartbeat.GetLastDeliveredMarketSequence(), v.sequence)
			}
			continue
		}
		if e.GetDelta() == nil || e.GetDelta().GetMarketSequence() != want {
			return fmt.Errorf("book expected delta %d, got %v", want, e)
		}
		return v.apply(e.GetDelta())
	}
}

func (c *checker) openTradeStream(ctx context.Context, after *api.TradeStreamCursor, wantMode api.StreamTradesStarted_Mode, head uint64) (*connect.ServerStreamForClient[api.StreamTradesResponse], error) {
	s, err := c.streamRPC.StreamTrades(ctx, connect.NewRequest(&api.StreamTradesRequest{MarketId: marketSymbol, ResumeAfter: after}))
	if err != nil {
		return nil, err
	}
	first, err := receiveTrade(s)
	if err == nil {
		start := first.GetStarted()
		if start == nil || start.GetMode() != wantMode || start.GetHeadMarketSequence() != head ||
			start.GetMarketDefinitionVersion() != definition || start.GetReplayFloorMarketSequence() > head {
			err = fmt.Errorf("invalid StreamTrades started event: %v", first)
		}
	}
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func expectTradeTick(tick *md.TradeTick, sequence, ordinal uint64) error {
	if tick == nil || tick.GetMarketId() != marketSymbol || tick.GetSourceMarketId() != sourceID ||
		tick.GetMarketDefinitionVersion() != definition || tick.GetMatchSequence() != sequence || tick.GetEventOrdinal() != ordinal {
		return fmt.Errorf("trade identity/provenance mismatch: %v", tick)
	}
	var id string
	var priceUnits, baseAtoms, quoteAtoms uint64
	switch {
	case sequence == 5 && ordinal == 0:
		id, priceUnits, baseAtoms, quoteAtoms = tradeID5a, price, 2000, 2000
	case sequence == 5 && ordinal == 1:
		id, priceUnits, baseAtoms, quoteAtoms = tradeID5b, price+25, 2000, 2005
	default:
		return fmt.Errorf("unexpected trade cursor %d:%d", sequence, ordinal)
	}
	if tick.GetTradeId() != id || tick.GetPriceUnits() != priceUnits || tick.GetBaseQuantityAtoms() != baseAtoms ||
		tick.GetQuoteQuantityAtoms() != quoteAtoms || tick.GetAggressorSide() != order.OrderSide_ORDER_SIDE_BUY || tick.GetRecordedAt() == nil || tick.GetRecordedAt().CheckValid() != nil {
		return fmt.Errorf("trade %d:%d payload mismatch: %v", sequence, ordinal, tick)
	}
	return nil
}

func expectTradeEvent(s *connect.ServerStreamForClient[api.StreamTradesResponse], seq, ordinal uint64) error {
	for {
		e, err := receiveTrade(s)
		if err != nil {
			return err
		}
		if e.GetHeartbeat() != nil {
			continue
		}
		if e.GetTrade() == nil {
			return fmt.Errorf("expected trade %d:%d, got %v", seq, ordinal, e)
		}
		return expectTradeTick(e.GetTrade(), seq, ordinal)
	}
}

func expectTradeSequenceEnd(s *connect.ServerStreamForClient[api.StreamTradesResponse], seq uint64) error {
	for {
		e, err := receiveTrade(s)
		if err != nil {
			return err
		}
		if e.GetHeartbeat() != nil {
			continue
		}
		end := e.GetSequenceEnd()
		if end == nil || end.GetMarketSequence() != seq || end.GetMarketDefinitionVersion() != definition {
			return fmt.Errorf("expected complete trade sequence %d, got %v", seq, e)
		}
		return nil
	}
}

func expectTickerState(ticker *md.Ticker, seq uint64) error {
	if ticker == nil || ticker.GetMarketId() != marketSymbol || ticker.GetMarketSequence() != seq ||
		ticker.GetUpdatedAt() == nil || ticker.GetUpdatedAt().CheckValid() != nil || ticker.BestBidPriceUnits != nil {
		return fmt.Errorf("invalid ticker at sequence %d: %v", seq, ticker)
	}
	switch seq {
	case 3, 4:
		if ticker.LastPriceUnits == nil || ticker.GetLastPriceUnits() != price || ticker.BestAskPriceUnits == nil || ticker.GetBestAskPriceUnits() != price {
			return fmt.Errorf("incorrect resting-book ticker at %d: %v", seq, ticker)
		}
	case 5:
		if ticker.LastPriceUnits == nil || ticker.GetLastPriceUnits() != price+25 || ticker.BestAskPriceUnits != nil {
			return fmt.Errorf("incorrect fully crossed ticker at %d: %v", seq, ticker)
		}
	default:
		return fmt.Errorf("unexpected ticker sequence %d", seq)
	}
	return nil
}

func (c *checker) openTickerStream(ctx context.Context, seq uint64) (*connect.ServerStreamForClient[api.StreamTickerResponse], error) {
	s, err := c.streamRPC.StreamTicker(ctx, connect.NewRequest(&api.StreamTickerRequest{MarketId: marketSymbol}))
	if err != nil {
		return nil, err
	}
	first, err := receiveTicker(s)
	if err == nil {
		if first.GetSnapshot() == nil || first.GetMarketDefinitionVersion() != definition {
			err = fmt.Errorf("first ticker event must be versioned snapshot: %v", first)
		} else {
			err = expectTickerState(first.GetSnapshot(), seq)
		}
	}
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// Ticker may coalesce any intermediate market sequence, never regress or
// repeat one. A deterministic E2E test cannot demand that a particular update
// is skipped: it depends on scheduling. The hub's unit test forces coalescing.
func expectTickerHead(s *connect.ServerStreamForClient[api.StreamTickerResponse], from, head uint64) (bool, error) {
	cursor := from
	skipped := false
	for cursor < head {
		e, err := receiveTicker(s)
		if err != nil {
			return skipped, err
		}
		if e.GetMarketDefinitionVersion() != definition {
			return skipped, fmt.Errorf("ticker definition version changed: %v", e)
		}
		if heartbeat := e.GetHeartbeat(); heartbeat != nil {
			if heartbeat.GetLastDeliveredMarketSequence() != cursor {
				return skipped, fmt.Errorf("ticker heartbeat cursor %d want %d", heartbeat.GetLastDeliveredMarketSequence(), cursor)
			}
			continue
		}
		updated := e.GetUpdate()
		if updated == nil || updated.GetMarketSequence() <= cursor || updated.GetMarketSequence() > head {
			return skipped, fmt.Errorf("ticker non-monotonic / unexpected update, cursor=%d event=%v", cursor, e)
		}
		if err := expectTickerState(updated, updated.GetMarketSequence()); err != nil {
			return skipped, err
		}
		if updated.GetMarketSequence() > cursor+1 {
			skipped = true
		}
		cursor = updated.GetMarketSequence()
	}
	return skipped, nil
}

func (c *checker) pgTradeSequenceFive() error {
	var nextOffset int64
	if err := c.db.QueryRowContext(c.ctx, `SELECT next_offset FROM market_data.partition_cursors
		WHERE topic=$1 AND partition_id=0`, c.topic).Scan(&nextOffset); err != nil || nextOffset != 5 {
		return fmt.Errorf("durable Kafka partition cursor=%d want 5: %v", nextOffset, err)
	}
	var count int
	if err := c.db.QueryRowContext(c.ctx, `SELECT count(*) FROM market_data.book_deltas
		WHERE topic=$1 AND source_market_id=$2`, c.topic, int64(sourceID)).Scan(&count); err != nil || count != 5 {
		return fmt.Errorf("durable book delta count=%d want 5: %v", count, err)
	}
	if err := c.db.QueryRowContext(c.ctx, `SELECT count(*) FROM market_data.public_trades
		WHERE topic=$1 AND source_market_id=$2`, c.topic, int64(sourceID)).Scan(&count); err != nil || count != 4 {
		return fmt.Errorf("durable execution count=%d want 4: %v", count, err)
	}
	var raw []byte
	if err := c.db.QueryRowContext(c.ctx, `SELECT snapshot FROM market_data.public_books
		WHERE topic=$1 AND source_market_id=$2`, c.topic, int64(sourceID)).Scan(&raw); err != nil {
		return err
	}
	book := new(md.BookSnapshot)
	if err := proto.Unmarshal(raw, book); err != nil {
		return err
	}
	if _, err := viewFromSnapshot(book, 5); err != nil {
		return err
	}
	rows, err := c.db.QueryContext(c.ctx, `SELECT trade_id, event_ordinal, payload
		FROM market_data.public_trades WHERE topic=$1 AND source_market_id=$2
		AND market_sequence=5 ORDER BY event_ordinal`, c.topic, int64(sourceID))
	if err != nil {
		return err
	}
	defer rows.Close()
	ordinal := uint64(0)
	for rows.Next() {
		var id string
		var ord uint64
		var bytes []byte
		if err := rows.Scan(&id, &ord, &bytes); err != nil {
			return err
		}
		tick := new(md.TradeTick)
		if err := proto.Unmarshal(bytes, tick); err != nil {
			return err
		}
		if ord != ordinal || id != tick.GetTradeId() {
			return fmt.Errorf("durable trade ordering failed: ordinal=%d id=%s", ord, id)
		}
		if err := expectTradeTick(tick, 5, ordinal); err != nil {
			return err
		}
		ordinal++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if ordinal != 2 {
		return fmt.Errorf("expected two durable executions in matching result 5, got %d", ordinal)
	}
	return nil
}

func (c *checker) verifyConnectStreams() error {
	if err := c.pgState(3); err != nil {
		return fmt.Errorf("stream phase requires durable baseline seq3: %w", err)
	}
	ctx, cancel := context.WithTimeout(c.ctx, streamingE2ETimeout)
	defer cancel()

	book, view, err := c.openBookStream(ctx, 3)
	if err != nil {
		return err
	}
	defer book.Close()
	tradeCtx, tradeCancel := context.WithCancel(ctx)
	defer tradeCancel()
	trades, err := c.openTradeStream(tradeCtx, nil, api.StreamTradesStarted_MODE_LIVE, 3)
	if err != nil {
		return err
	}
	defer trades.Close()
	ticker, err := c.openTickerStream(ctx, 3)
	if err != nil {
		return err
	}
	defer ticker.Close()
	fmt.Println("PASS: all three Connect streams established at durable sequence 3 (book/ticker snapshots and trades started LIVE)")

	// Publish and persist both sequences before consuming queued network
	// updates. Depending on hub scheduling, ticker may coalesce 3 -> 5, or
	// emit 3 -> 4 -> 5; BOTH are correct latest-state semantics.
	if err := c.produce(4); err != nil {
		return err
	}
	if err := c.produce(5); err != nil {
		return err
	}
	if err := c.wait("PostgreSQL durable two-execution result seq5", c.pgTradeSequenceFive); err != nil {
		return err
	}
	if err := expectBookNext(book, view, 4); err != nil {
		return err
	}
	if err := expectBookNext(book, view, 5); err != nil {
		return err
	}
	fmt.Println("PASS: StreamOrderBook snapshot + contiguous absolute L2 deltas 3->4->5")

	if err := expectTradeEvent(trades, 5, 0); err != nil {
		return err
	}
	// Disconnect BEFORE reading the second execution or sequence_end. Resume
	// from the exact committed trade, not a fabricated sequence cursor.
	tradeCancel() // cancel the server's subscriber, not just the response reader
	_ = trades.Close()
	cursor := &api.TradeStreamCursor{MarketSequence: 5, EventOrdinal: 0,
		SequenceComplete: false, MarketDefinitionVersion: definition}
	replayed, err := c.openTradeStream(ctx, cursor, api.StreamTradesStarted_MODE_REPLAY, 5)
	if err != nil {
		return err
	}
	defer replayed.Close()
	if err := expectTradeEvent(replayed, 5, 1); err != nil {
		return err
	}
	if err := expectTradeSequenceEnd(replayed, 5); err != nil {
		return err
	}
	fmt.Println("PASS: StreamTrades LIVE 5:0 -> disconnect -> REPLAY 5:1 + sequence_end, no duplicate 5:0")

	skipped, err := expectTickerHead(ticker, 3, 5)
	if err != nil {
		return err
	}
	fmt.Printf("PASS: StreamTicker latest-state monotonic 3->5 (coalesced intermediate sequence: %t)\n", skipped)
	return nil
}

// This phase runs as a child process while e2e.sh SIGTERMs the real server.
// The ready file is emitted ONLY after all streams have successfully received
// their first event, so the test cannot accidentally pass without subscribers.
func (c *checker) verifyStreamDisconnect(readyFile, stopFile string) error {
	if err := c.pgTradeSequenceFive(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	book, _, err := c.openBookStream(ctx, 5)
	if err != nil {
		return err
	}
	defer book.Close()
	trades, err := c.openTradeStream(ctx, nil, api.StreamTradesStarted_MODE_LIVE, 5)
	if err != nil {
		return err
	}
	defer trades.Close()
	ticker, err := c.openTickerStream(ctx, 5)
	if err != nil {
		return err
	}
	defer ticker.Close()

	// Drain in parallel: shutdown must release EVERY kind of active stream.
	outcomes := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(3)
	watch := func(name string, read func() error) {
		defer wg.Done()
		if err := read(); err == nil {
			outcomes <- fmt.Errorf("%s returned another event after shutdown request", name)
		} else if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || connect.CodeOf(err) == connect.CodeDeadlineExceeded {
			outcomes <- fmt.Errorf("%s only ended on E2E timeout: %v", name, err)
		} else if _, statErr := os.Stat(stopFile); statErr != nil {
			outcomes <- fmt.Errorf("%s terminated BEFORE backend shutdown was requested: %v", name, err)
		} else {
			outcomes <- nil
		}
	}
	go watch("book", func() error { _, e := receiveBook(book); return e })
	go watch("trades", func() error { _, e := receiveTrade(trades); return e })
	go watch("ticker", func() error { _, e := receiveTicker(ticker); return e })

	// Atomic readiness prevents the controlling script seeing a partial file.
	if err := os.WriteFile(readyFile+".tmp", []byte("three Connect streams open\n"), 0600); err != nil {
		cancel()
		wg.Wait()
		return err
	}
	if err := os.Rename(readyFile+".tmp", readyFile); err != nil {
		cancel()
		wg.Wait()
		return err
	}
	for i := 0; i < 3; i++ {
		if err := <-outcomes; err != nil {
			cancel()
			wg.Wait()
			return err
		}
	}
	wg.Wait()
	fmt.Println("PASS: server SIGTERM terminated all three active Connect streams before the client deadline")
	return nil
}

// A complete restart must not depend on ephemeral hub state. A fresh book and
// ticker reset at durable seq5; a trade resume from a specific event replays
// only the remaining execution from that matching sequence.
func (c *checker) verifyStreamReconnect() error {
	if err := c.pgTradeSequenceFive(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.ctx, streamingE2ETimeout)
	defer cancel()
	book, _, err := c.openBookStream(ctx, 5)
	if err != nil {
		return err
	}
	defer book.Close()
	ticker, err := c.openTickerStream(ctx, 5)
	if err != nil {
		return err
	}
	defer ticker.Close()
	resume := &api.TradeStreamCursor{MarketSequence: 5, EventOrdinal: 0, MarketDefinitionVersion: definition}
	trades, err := c.openTradeStream(ctx, resume, api.StreamTradesStarted_MODE_REPLAY, 5)
	if err != nil {
		return err
	}
	defer trades.Close()
	if err := expectTradeEvent(trades, 5, 1); err != nil {
		return err
	}
	if err := expectTradeSequenceEnd(trades, 5); err != nil {
		return err
	}
	fmt.Println("PASS: after server restart, book/ticker reset to fresh PG seq5 and trades resume exactly at 5:1")
	return nil
}
