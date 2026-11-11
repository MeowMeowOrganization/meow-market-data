//go:build integration

package readmodel_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/statistics"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The fixture mixes exact window edges, an out-of-order recorded timestamp,
// a complete interior minute and excluded trades. PostgreSQL computes the
// partial boundary aggregates; the durable minute row covers the interior.
func TestRollingStatsMinuteAndBoundaryParity(t *testing.T) {
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL with migrations through 000004")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('market_data.trade_minute_rollups') IS NOT NULL`).Scan(&exists); err != nil || !exists {
		t.Fatalf("migration 000004 required: %v", err)
	}
	topic := fmt.Sprintf("stats-24h-%d", time.Now().UnixNano())
	asof := time.Date(2026, 11, 11, 12, 34, 30, 0, time.UTC)
	start := asof.Add(-24 * time.Hour)
	symbol := "BTC-USDT"
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	snap := &md.BookSnapshot{SourceMarketId: 7, MarketId: symbol, MarketSequence: 9, MarketDefinitionVersion: 3, Depth: 20}
	encoded, err := proto.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO market_data.public_books(topic,source_market_id,market_id,market_sequence,
      market_definition_version,depth,recorded_at_ns,snapshot) VALUES($1,7,$2,9,3,20,$3,$4)`,
		topic, symbol, postgres.NanosDecimal(asof), encoded)
	if err != nil {
		t.Fatal(err)
	}
	type tradeCase struct {
		seq          uint64
		at           time.Time
		price, atoms uint64
	}
	cases := []tradeCase{
		{1, start, 10000, 1}, // excluded from first window, included in earlier-asof window
		{2, start.Add(5 * time.Second), 9000, 2},
		{3, start.Add(20 * time.Second), 11000, 3},
		{4, start.Truncate(time.Minute).Add(time.Minute + 15*time.Second), 8000, 4},
		{5, asof.Truncate(time.Minute).Add(20 * time.Second), 12000, 5},
		{6, asof, 10000, 6},
		{7, asof.Add(time.Nanosecond), 7000, 7},                                      // excluded
		{8, start.Add(-time.Minute), 6000, 8},                                        // excluded
		{9, start.Truncate(time.Minute).Add(time.Minute + 10*time.Second), 13000, 9}, // late event recorded before seq4
	}
	ticks := make([]*md.TradeTick, 0, len(cases))
	for _, tc := range cases {
		tick := &md.TradeTick{SourceMarketId: 7, MarketId: symbol, MatchSequence: tc.seq, EventOrdinal: 0,
			TradeId: fmt.Sprintf("%s:%d", topic, tc.seq), PriceUnits: tc.price, BaseQuantityAtoms: tc.atoms,
			QuoteQuantityAtoms: tc.atoms * 10, RecordedAt: timestamppb.New(tc.at)}
		payload, err := proto.Marshal(tick)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO market_data.public_trades
          (topic,source_market_id,market_id,market_sequence,event_ordinal,trade_id,recorded_at_ns,price_units,base_atoms,quote_atoms,payload)
          VALUES ($1,7,$2,$3,0,$4,$5,$6,$7,$8,$9)`, topic, symbol,
			fmt.Sprint(tc.seq), tick.GetTradeId(), postgres.NanosDecimal(tc.at), fmt.Sprint(tc.price), fmt.Sprint(tc.atoms),
			fmt.Sprint(tc.atoms*10), payload)
		if err != nil {
			t.Fatal(err)
		}
		ticks = append(ticks, tick)
	}
	// The only fully covered minute in this sparse fixture contains seq4 and
	// seq9. Their SOURCE order is 4->9 despite recorded time being 15s->10s.
	at := start.Truncate(time.Minute).Add(time.Minute)
	_, err = tx.ExecContext(ctx, `INSERT INTO market_data.trade_minute_rollups
       (topic,source_market_id,minute_start_ns,market_id,
        first_sequence,first_ordinal,first_price_units,last_sequence,last_ordinal,last_price_units,
        high_price_units,low_price_units,base_volume_atoms,quote_volume_atoms,trade_count)
       VALUES($1,7,$2,$3,4,0,8000,9,0,13000,13000,8000,13,130,2)`, topic, postgres.NanosDecimal(at), symbol)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		for _, table := range []string{"trade_minute_rollups", "public_trades", "public_books"} {
			_, _ = db.ExecContext(clean, `DELETE FROM market_data.`+table+` WHERE topic=$1`, topic)
		}
	})
	reader := &readmodel.Reader{DB: db, Topic: topic}
	for _, now := range []time.Time{asof, asof.Add(-10 * time.Second), asof.Add(-30 * time.Second)} {
		var window []*md.TradeTick
		for _, tick := range ticks {
			at := tick.GetRecordedAt().AsTime()
			if at.After(now.Add(-24*time.Hour)) && !at.After(now) {
				window = append(window, tick)
			}
		}
		var reference *uint64
		cutoff := now.Add(-24 * time.Hour)
		var referenceStamp time.Time
		var referenceSeq uint64
		for _, tick := range ticks {
			stamp := tick.GetRecordedAt().AsTime()
			if stamp.After(cutoff) {
				continue
			}
			if reference == nil || stamp.After(referenceStamp) || (stamp.Equal(referenceStamp) && tick.GetMatchSequence() > referenceSeq) {
				p := tick.GetPriceUnits()
				reference = &p
				referenceStamp = stamp
				referenceSeq = tick.GetMatchSequence()
			}
		}
		var expected *md.Stats24H
		var err error
		if reference == nil {
			expected, err = statistics.Stats24h(symbol, now, window)
		} else {
			expected, err = statistics.Stats24h(symbol, now, window, *reference)
		}
		if err != nil {
			t.Fatal(err)
		}
		got, err := reader.LoadStats(ctx, 7, now)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(got, expected) {
			t.Fatalf("minute parity at %s: got=%v want=%v", now, got, expected)
		}
	}
}

// Benchmark consumes an existing test topic. No synthetic millions of rows
// are manufactured as part of the timing, and SQL history remains untouched.
func BenchmarkLoadStats24hPostgres(b *testing.B) {
	dsn, topic := os.Getenv("MARKET_DATA_TEST_DATABASE_URL"), os.Getenv("MARKET_DATA_PERF_TOPIC")
	if dsn == "" || topic == "" {
		b.Skip("set MARKET_DATA_TEST_DATABASE_URL and MARKET_DATA_PERF_TOPIC")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	reader := &readmodel.Reader{DB: db, Topic: topic}
	var id int64
	if err := db.QueryRow(`SELECT source_market_id FROM market_data.public_books WHERE topic=$1 LIMIT 1`, topic).Scan(&id); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()

	for b.Loop() {
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := reader.LoadStats(ctx, uint32(id), time.Now().UTC())
		stop()
		if err != nil {
			b.Fatal(err)
		}
	}
}
