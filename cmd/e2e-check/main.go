// Command e2e-check verifies CEX 23.7 through the real running service.
// It intentionally never imports internal projector/recovery packages: the
// only write input is Kafka, and all observations are external interfaces.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
	"github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1/apiv1alpha1connect"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matching "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	order "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	marketSymbol = "BTC-USDT"
	sourceID     = uint32(7)
	definition   = uint64(3)
	price        = uint64(10000) // 400 ticks * 25 price units
	tradeID2     = "7:2:0"
	tradeID3     = "7:3:0"
)

type checker struct {
	ctx     context.Context
	db      *sql.DB
	redis   *redis.Client
	kafka   *kgo.Client
	topic   string
	httpURL string
	rpc     apiv1alpha1connect.MarketDataServiceClient
}

func main() {
	phase := flag.String("phase", "live", "live or recovery")
	dsn := flag.String("dsn", "", "isolated PostgreSQL database URL")
	redisURL := flag.String("redis", "", "isolated Redis URL")
	brokers := flag.String("brokers", "127.0.0.1:9094", "Kafka bootstrap brokers")
	topic := flag.String("topic", "", "fresh one-partition matching-result topic")
	httpURL := flag.String("http", "http://127.0.0.1:18090", "running market-data listener")
	flag.Parse()
	if *dsn == "" || *redisURL == "" || *topic == "" || (*phase != "live" && *phase != "recovery") {
		log.Fatal("--phase live|recovery, --dsn, --redis, and --topic are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", *dsn)
	must(err)
	defer db.Close()
	opts, err := redis.ParseURL(*redisURL)
	must(err)
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	must(err)
	defer producer.Close()
	must(db.PingContext(ctx))
	must(rdb.Ping(ctx).Err())
	must(producer.Ping(ctx))
	c := &checker{ctx: ctx, db: db, redis: rdb, kafka: producer, topic: *topic,
		httpURL: strings.TrimRight(*httpURL, "/"),
		rpc:     apiv1alpha1connect.NewMarketDataServiceClient(&http.Client{Timeout: 8 * time.Second}, strings.TrimRight(*httpURL, "/"))}
	if *phase == "live" {
		must(c.live())
	} else {
		must(c.recovery())
	}
	fmt.Println("PASS: phase", *phase)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func (c *checker) wait(name string, check func() error) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		if last = check(); last == nil {
			fmt.Println("PASS:", name)
			return nil
		}
		select {
		case <-c.ctx.Done():
			return fmt.Errorf("%s: %w (last observed: %v)", name, c.ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func envelope(seq uint64, recordedAt time.Time) *matching.CommittedMatchingResult {
	cmd := &matching.MatchingCommand{MarketId: sourceID, CommandId: fmt.Sprintf("e2e-%d", seq)}
	var events []*matching.SequencedMatchingEvent
	switch seq {
	case 1:
		cmd.Command = &matching.MatchingCommand_PlaceLimitOrder{PlaceLimitOrder: &matching.PlaceLimitOrder{
			OrderId: 1001, Side: order.OrderSide_ORDER_SIDE_SELL, PriceTicks: 400, QuantityLots: 5,
		}}
	case 2, 3:
		lots := int64(2)
		if seq == 3 {
			lots = 1
		}
		takerOrder := uint64(1000 + seq)
		cmd.Command = &matching.MatchingCommand_PlaceLimitOrder{PlaceLimitOrder: &matching.PlaceLimitOrder{
			OrderId: takerOrder, Side: order.OrderSide_ORDER_SIDE_BUY, PriceTicks: 401, QuantityLots: lots,
		}}
		events = []*matching.SequencedMatchingEvent{{
			EventId: &matching.MatchingEventId{MarketId: sourceID, MarketSequence: seq, EventOrdinal: 0},
			// The source matching protobuf field is intentionally spelled Exectuion.
			Event: &matching.SequencedMatchingEvent_Exectuion{Exectuion: &matching.Execution{
				MakerOrderId: 1001, TakerOrderId: takerOrder, TakerSide: order.OrderSide_ORDER_SIDE_BUY,
				PriceTicks: 400, QuantityLots: lots,
			}},
		}}
	default:
		panic("unsupported e2e matching sequence")
	}
	return &matching.CommittedMatchingResult{
		Result:     &matching.MatchingResult{Command: cmd, MarketSequence: seq, Events: events},
		RecordedAt: timestamppb.New(recordedAt.UTC()), MarketDefinitionVersion: definition,
	}
}

func (c *checker) produce(seq uint64) error {
	data, err := proto.Marshal(envelope(seq, time.Now().UTC().Add(-time.Second)))
	if err != nil {
		return err
	}
	// Explicit partition 0 and canonical key match the configured market registry.
	record := &kgo.Record{Topic: c.topic, Partition: 0, Key: []byte("7"), Value: data}
	if err := c.kafka.ProduceSync(c.ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce matching result %d: %w", seq, err)
	}
	fmt.Printf("PASS: Kafka published CommittedMatchingResult sequence %d\n", seq)
	return nil
}

func (c *checker) pgState(wantSeq uint64) error {
	var next int64
	err := c.db.QueryRowContext(c.ctx, `SELECT next_offset FROM market_data.partition_cursors
		WHERE topic=$1 AND partition_id=0`, c.topic).Scan(&next)
	if err != nil {
		return err
	}
	if next != int64(wantSeq) {
		return fmt.Errorf("PG partition next_offset=%d want %d", next, wantSeq)
	}
	var checkpointCount int
	err = c.db.QueryRowContext(c.ctx, `SELECT COUNT(*) FROM market_data.book_checkpoints
		WHERE topic=$1 AND partition_id=0 AND source_market_id=7`, c.topic).Scan(&checkpointCount)
	if err != nil || checkpointCount != 1 {
		return fmt.Errorf("checkpoint count=%d: %v", checkpointCount, err)
	}
	var raw []byte
	err = c.db.QueryRowContext(c.ctx, `SELECT snapshot FROM market_data.public_books
		WHERE topic=$1 AND source_market_id=7`, c.topic).Scan(&raw)
	if err != nil {
		return err
	}
	snap := new(md.BookSnapshot)
	if err := proto.Unmarshal(raw, snap); err != nil {
		return err
	}
	if err := checkBook(snap, wantSeq); err != nil {
		return err
	}
	var deltas, trades, candleRevisions int
	err = c.db.QueryRowContext(c.ctx, `SELECT COUNT(*) FROM market_data.book_deltas
		WHERE topic=$1 AND source_market_id=7`, c.topic).Scan(&deltas)
	if err != nil || deltas != int(wantSeq) {
		return fmt.Errorf("delta count=%d: %v", deltas, err)
	}
	err = c.db.QueryRowContext(c.ctx, `SELECT COUNT(*) FROM market_data.public_trades
		WHERE topic=$1 AND source_market_id=7`, c.topic).Scan(&trades)
	if err != nil || trades != int(wantSeq)-1 {
		return fmt.Errorf("trade count=%d: %v", trades, err)
	}
	err = c.db.QueryRowContext(c.ctx, `SELECT COUNT(*) FROM market_data.candle_updates
		WHERE topic=$1 AND source_market_id=7`, c.topic).Scan(&candleRevisions)
	if err != nil || candleRevisions != (int(wantSeq)-1)*5 {
		return fmt.Errorf("candle revision count=%d: %v", candleRevisions, err)
	}
	if wantSeq >= 2 {
		var id, quoteAtoms string
		err = c.db.QueryRowContext(c.ctx, `SELECT trade_id,quote_atoms::text FROM market_data.public_trades
			WHERE topic=$1 AND source_market_id=7 AND market_sequence=2 AND event_ordinal=0`, c.topic).Scan(&id, &quoteAtoms)
		if err != nil || id != tradeID2 || quoteAtoms != "2000" {
			return fmt.Errorf("canonical trade provenance/quote: id=%q quote=%q err=%v", id, quoteAtoms, err)
		}
	}
	return nil
}

func checkBook(s *md.BookSnapshot, seq uint64) error {
	wantLots := map[uint64]string{1: "5000", 2: "3000", 3: "2000"}
	if s == nil || s.GetMarketId() != marketSymbol || s.GetSourceMarketId() != sourceID ||
		s.GetMarketDefinitionVersion() != definition || s.GetMarketSequence() != seq ||
		len(s.GetBids()) != 0 || len(s.GetAsks()) != 1 ||
		s.GetAsks()[0].GetPriceUnits() != price || s.GetAsks()[0].GetBaseQuantityAtoms() != wantLots[seq] {
		return fmt.Errorf("book sequence %d mismatch: %v", seq, s)
	}
	return nil
}

func (c *checker) redisState(seq uint64) error {
	prefix := "md:v1:m:{7}:"
	stored, err := c.redis.HGet(c.ctx, prefix+"state", "sequence").Result()
	if err != nil || stored != strconv.FormatUint(seq, 10) {
		return fmt.Errorf("Redis sequence %q want %d: %v", stored, seq, err)
	}
	bookRaw, err := c.redis.Get(c.ctx, prefix+"book:top").Bytes()
	if err != nil {
		return err
	}
	snap := new(md.BookSnapshot)
	if err := protojson.Unmarshal(bookRaw, snap); err != nil {
		return err
	}
	if err := checkBook(snap, seq); err != nil {
		return err
	}
	if seq >= 2 {
		tradesRaw, err := c.redis.Get(c.ctx, prefix+"trades:latest").Bytes()
		if err != nil {
			return err
		}
		var trades []json.RawMessage
		if err = json.Unmarshal(tradesRaw, &trades); err != nil || len(trades) < int(seq)-1 {
			return fmt.Errorf("Redis recent trades missing: %v (%v)", string(tradesRaw), err)
		}
		tickerRaw, err := c.redis.Get(c.ctx, prefix+"ticker").Bytes()
		if err != nil {
			return err
		}
		ticker := new(md.Ticker)
		if err := protojson.Unmarshal(tickerRaw, ticker); err != nil || ticker.GetLastPriceUnits() != price {
			return fmt.Errorf("Redis ticker mismatch: %v (%v)", ticker, err)
		}
		statsRaw, err := c.redis.Get(c.ctx, prefix+"stats:24h").Bytes()
		if err != nil {
			return err
		}
		stats := new(md.Stats24H)
		if err := protojson.Unmarshal(statsRaw, stats); err != nil || stats.GetBaseVolumeAtoms() != fmt.Sprint(seq*1000) {
			// Sequence 2 = 2000 atoms, sequence 3 = 3000 atoms.
			return fmt.Errorf("Redis rolling stats mismatch: %v (%v)", stats, err)
		}
		candleRaw, err := c.redis.Get(c.ctx, prefix+"candles:1m").Bytes()
		if err != nil {
			return err
		}
		var candles []json.RawMessage
		if err = json.Unmarshal(candleRaw, &candles); err != nil || len(candles) == 0 {
			return fmt.Errorf("Redis candles missing: %v (%v)", string(candleRaw), err)
		}
	}
	return nil
}

func (c *checker) rpcState(seq uint64) error {
	book, err := c.rpc.GetOrderBook(c.ctx, connect.NewRequest(&api.GetOrderBookRequest{MarketId: marketSymbol, Depth: 20}))
	if err != nil {
		return err
	}
	if err := checkBook(book.Msg.GetBook(), seq); err != nil {
		return err
	}
	if seq < 2 {
		return nil
	}
	trades, err := c.rpc.ListRecentTrades(c.ctx, connect.NewRequest(&api.ListRecentTradesRequest{MarketId: marketSymbol, Limit: 10}))
	if err != nil {
		return err
	}
	if len(trades.Msg.GetTrades()) != int(seq)-1 {
		return fmt.Errorf("Connect trades count=%d", len(trades.Msg.GetTrades()))
	}
	latest := trades.Msg.GetTrades()[0]
	wantTrade := tradeID2
	if seq == 3 {
		wantTrade = tradeID3
	}
	if latest.GetTradeId() != wantTrade || latest.GetPriceUnits() != price {
		return fmt.Errorf("Connect latest trade=%v", latest)
	}
	ticker, err := c.rpc.GetTicker(c.ctx, connect.NewRequest(&api.GetTickerRequest{MarketId: marketSymbol}))
	if err != nil {
		return err
	}
	if ticker.Msg.GetTicker().GetLastPriceUnits() != price || ticker.Msg.GetTicker().GetMarketSequence() != seq ||
		ticker.Msg.GetTicker().GetBestAskPriceUnits() != price {
		return fmt.Errorf("Connect ticker mismatch: %v", ticker.Msg.GetTicker())
	}
	stats, err := c.rpc.GetStats24H(c.ctx, connect.NewRequest(&api.GetStats24HRequest{MarketId: marketSymbol}))
	if err != nil {
		return err
	}
	wantVolume := "2000"
	if seq == 3 {
		wantVolume = "3000"
	}
	if stats.Msg.GetStats() == nil || stats.Msg.GetStats().GetBaseVolumeAtoms() != wantVolume {
		return fmt.Errorf("Connect rolling statistics mismatch: %v", stats.Msg.GetStats())
	}
	candles, err := c.rpc.ListCandles(c.ctx, connect.NewRequest(&api.ListCandlesRequest{
		MarketId: marketSymbol, Interval: md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE,
		StartTimeInclusive: timestamppb.New(time.Now().UTC().Add(-time.Hour)),
		EndTimeExclusive:   timestamppb.New(time.Now().UTC().Add(time.Hour)), Limit: 20,
	}))
	if err != nil {
		return err
	}
	var total uint64
	for _, candle := range candles.Msg.GetCandles() {
		total += candle.GetTradeCount()
	}
	if total != seq-1 {
		return fmt.Errorf("Connect candle trade count=%d want %d", total, seq-1)
	}
	return nil
}

type frame struct {
	Type           string          `json:"type"`
	Mode           string          `json:"mode"`
	MarketSequence string          `json:"marketSequence"`
	Data           json.RawMessage `json:"data"`
}

func (c *checker) openWS(replay bool) (*websocket.Conn, error) {
	endpoint := strings.Replace(c.httpURL, "http://", "ws://", 1)
	endpoint = strings.Replace(endpoint, "https://", "wss://", 1)
	u, err := url.Parse(endpoint + "/v1/market-data/stream")
	if err != nil {
		return nil, err
	}
	query := url.Values{"market_id": {marketSymbol}, "depth": {"20"}, "channels": {"book,trades,candles"}}
	if replay {
		query.Set("after_sequence", "1")
		query.Set("market_definition_version", "3")
	}
	u.RawQuery = query.Encode()
	ws, resp, err := websocket.DefaultDialer.DialContext(c.ctx, u.String(), nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("websocket HTTP %d: %w", resp.StatusCode, err)
		}
		return nil, err
	}
	f, err := readFrame(ws)
	if err != nil {
		ws.Close()
		return nil, err
	}
	mode := "snapshot"
	if replay {
		mode = "replay"
	}
	if f.Type != "ready" || f.Mode != mode {
		ws.Close()
		return nil, fmt.Errorf("unexpected WS ready: %+v", f)
	}
	if !replay {
		f, err = readFrame(ws)
		if err != nil {
			ws.Close()
			return nil, err
		}
		if f.Type != "update" {
			ws.Close()
			return nil, fmt.Errorf("expected initial book snapshot; got %+v", f)
		}
		u := new(md.MarketDataUpdate)
		if err := protojson.Unmarshal(f.Data, u); err != nil || checkBook(u.GetBookSnapshot(), 1) != nil {
			ws.Close()
			return nil, fmt.Errorf("initial WS snapshot invalid: %v (%v)", u, err)
		}
		f, err = readFrame(ws)
		if err != nil {
			ws.Close()
			return nil, err
		}
		if f.Type != "sequence_end" || f.MarketSequence != "1" {
			ws.Close()
			return nil, fmt.Errorf("initial WS cursor mismatch: %+v", f)
		}
	}
	fmt.Println("PASS: WebSocket ready mode", mode)
	return ws, nil
}

func readFrame(ws *websocket.Conn) (frame, error) {
	var f frame
	if err := ws.SetReadDeadline(time.Now().Add(45 * time.Second)); err != nil {
		return f, err
	}
	_, data, err := ws.ReadMessage()
	if err != nil {
		return f, err
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return f, err
	}
	if f.Type == "resync_required" {
		return f, errors.New("websocket requested resync unexpectedly")
	}
	return f, nil
}

func (c *checker) expectWSSequence(ws *websocket.Conn, seq uint64) error {
	var gotDelta, gotTrade bool
	candles := make(map[md.CandleInterval]bool)
	for {
		f, err := readFrame(ws)
		if err != nil {
			return err
		}
		if f.Type == "sequence_end" {
			if f.MarketSequence != strconv.FormatUint(seq, 10) || !gotDelta || !gotTrade || len(candles) != 5 {
				return fmt.Errorf("WS sequence %d incomplete: delta=%v trade=%v candles=%d end=%s", seq, gotDelta, gotTrade, len(candles), f.MarketSequence)
			}
			fmt.Printf("PASS: WebSocket complete sequence %d (delta, execution, five candle updates, sequence_end)\n", seq)
			return nil
		}
		if f.Type != "update" {
			return fmt.Errorf("unexpected WS frame type %q", f.Type)
		}
		u := new(md.MarketDataUpdate)
		if err := protojson.Unmarshal(f.Data, u); err != nil {
			return err
		}
		if delta := u.GetBookDelta(); delta != nil {
			if delta.GetMarketSequence() != seq || delta.GetPreviousMarketSequence() != seq-1 {
				return fmt.Errorf("WS delta sequence mismatch: %v", delta)
			}
			gotDelta = true
		}
		if trade := u.GetTrade(); trade != nil {
			wantTrade, wantAtoms := tradeID2, uint64(2000)
			if seq == 3 {
				wantTrade, wantAtoms = tradeID3, 1000
			}
			if trade.GetTradeId() != wantTrade || trade.GetMatchSequence() != seq || trade.GetBaseQuantityAtoms() != wantAtoms || trade.GetPriceUnits() != price {
				return fmt.Errorf("WS trade mismatch: %v", trade)
			}
			gotTrade = true
		}
		if candle := u.GetCandle(); candle != nil {
			if candle.GetMarketId() != marketSymbol || candle.GetTradeCount() == 0 {
				return fmt.Errorf("WS candle mismatch: %v", candle)
			}
			candles[candle.GetInterval()] = true
		}
	}
}

func (c *checker) live() error {
	if err := c.produce(1); err != nil {
		return err
	}
	if err := c.wait("PostgreSQL durable seq=1", func() error { return c.pgState(1) }); err != nil {
		return err
	}
	if err := c.wait("Redis seq=1", func() error { return c.redisState(1) }); err != nil {
		return err
	}
	if err := c.wait("Connect book seq=1", func() error { return c.rpcState(1) }); err != nil {
		return err
	}
	ws, err := c.openWS(false)
	if err != nil {
		return err
	}
	defer ws.Close()
	if err = c.produce(2); err != nil {
		return err
	}
	if err = c.expectWSSequence(ws, 2); err != nil {
		return err
	}
	if err = c.wait("PostgreSQL checkpoint, trade, candles seq=2", func() error { return c.pgState(2) }); err != nil {
		return err
	}
	if err = c.wait("Redis coherent caches seq=2", func() error { return c.redisState(2) }); err != nil {
		return err
	}
	return c.wait("all five Connect RPCs seq=2", func() error { return c.rpcState(2) })
}

func (c *checker) recovery() error {
	if err := c.wait("restored PostgreSQL seq=2", func() error { return c.pgState(2) }); err != nil {
		return err
	}
	ws, err := c.openWS(true)
	if err != nil {
		return err
	}
	defer ws.Close()
	if err := c.expectWSSequence(ws, 2); err != nil {
		return err
	}
	if err := c.produce(3); err != nil {
		return err
	}
	if err := c.expectWSSequence(ws, 3); err != nil {
		return err
	}
	if err := c.wait("PostgreSQL restored resting order and persisted seq=3", func() error { return c.pgState(3) }); err != nil {
		return err
	}
	if err := c.wait("Redis caches seq=3", func() error { return c.redisState(3) }); err != nil {
		return err
	}
	if err := c.wait("all five Connect RPCs seq=3", func() error { return c.rpcState(3) }); err != nil {
		return err
	}
	// Safe because scripts/e2e.sh runs a dedicated redis-e2e container.
	if err := c.redis.FlushDB(c.ctx).Err(); err != nil {
		return err
	}
	fmt.Println("PASS: deliberately evicted dedicated E2E Redis cache")
	if err := c.wait("Connect remains available after Redis cache eviction", func() error { return c.rpcState(3) }); err != nil {
		return err
	}
	return c.wait("Redis full recovery from PostgreSQL", func() error { return c.redisState(3) })
}
