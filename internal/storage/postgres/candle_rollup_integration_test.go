//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/proto"
)

// Proves candle materialization consumes minute rollups, not public_trades.
// Late corrections, revision increments and rollup changes are made in the
// SAME transaction as the candle, just as in Store.Save's fenced transaction.
func TestCandleFromDurableMinuteRollupsWithoutHistoricalTrades(t *testing.T) {
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires MARKET_DATA_TEST_DATABASE_URL and migrations through 000004")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, done := context.WithTimeout(context.Background(), 20*time.Second)
	defer done()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	var available sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('market_data.trade_minute_rollups')::text`).Scan(&available); err != nil || !available.Valid {
		t.Fatalf("apply migrations through 000004: %v", err)
	}
	store, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	topic := fmt.Sprintf("cex251-rollup-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, name := range []string{"candle_updates", "candles", "trade_minute_rollups"} {
			_, _ = db.ExecContext(clean, `DELETE FROM market_data.`+name+` WHERE topic=$1`, topic)
		}
	})
	start := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	const marketID uint32 = 7
	const symbol = "BTC-USDT"
	insertMinute := func(tx *sql.Tx, at time.Time, seq, price, base, quote uint64) {
		t.Helper()
		_, err := tx.ExecContext(ctx, `INSERT INTO market_data.trade_minute_rollups
            (topic,source_market_id,minute_start_ns,market_id,
             first_sequence,first_ordinal,first_price_units,
             last_sequence,last_ordinal,last_price_units,high_price_units,low_price_units,
             base_volume_atoms,quote_volume_atoms,trade_count)
            VALUES ($1,$2,$3,$4,$5,0,$6,$5,0,$6,$6,$6,$7,$8,1)`,
			topic, int64(marketID), NanosDecimal(at), symbol, fmt.Sprint(seq), fmt.Sprint(price), fmt.Sprint(base), fmt.Sprint(quote))
		if err != nil {
			t.Fatal(err)
		}
	}
	// An invalid rollup must fail closed. As in Store.Save, a caller can
	// roll back the whole transaction; no candle or rollup is left behind.
	failed, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	insertMinute(failed, start, 1, 90, 3, 6)
	if _, err := store.rebuildCandle(ctx, failed, topic, marketID, "WRONG-MARKET", md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, start); err == nil {
		_ = failed.Rollback()
		t.Fatal("incorrect symbol did not fail closed")
	}
	if err := failed.Rollback(); err != nil {
		t.Fatal(err)
	}
	var afterRollback int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM market_data.trade_minute_rollups WHERE topic=$1`, topic).Scan(&afterRollback); err != nil || afterRollback != 0 {
		t.Fatalf("rollup was not rolled back: count=%d err=%v", afterRollback, err)
	}

	// Start with a newer source event in minute 12:01, and then insert an
	// older source event into minute 12:00. Open/close must follow source order.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	insertMinute(tx, start.Add(time.Minute), 2, 110, 4, 8)
	first, err := store.rebuildCandle(ctx, tx, topic, marketID, symbol, md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES, start)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if first.GetOpenPriceUnits() != 110 || first.GetRevision() != 0 {
		t.Fatalf("initial candle=%v", first)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	insertMinute(tx, start, 1, 90, 3, 6)
	c, err := store.rebuildCandle(ctx, tx, topic, marketID, symbol, md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES, start)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if c.GetOpenPriceUnits() != 90 || c.GetClosePriceUnits() != 110 || c.GetLowPriceUnits() != 90 ||
		c.GetHighPriceUnits() != 110 || c.GetBaseVolumeAtoms() != "7" || c.GetQuoteVolumeAtoms() != "14" ||
		c.GetTradeCount() != 2 || c.GetRevision() != 1 {
		t.Fatalf("late minute correction=%v", c)
	}
	minute, err := store.rebuildCandle(ctx, tx, topic, marketID, symbol, md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE, start)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if minute.GetTradeCount() != 1 || minute.GetOpenPriceUnits() != 90 || minute.GetRevision() != 0 {
		t.Fatalf("1m candle=%v", minute)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM market_data.candles
        WHERE topic=$1 AND source_market_id=$2 AND interval_id=$3 AND start_ns=$4`,
		topic, int64(marketID), int32(md.CandleInterval_CANDLE_INTERVAL_FIVE_MINUTES), NanosDecimal(start)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	stored := new(md.Candle)
	if err := proto.Unmarshal(raw, stored); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(stored, c) {
		t.Fatalf("durable candle differs: got %v expected %v", stored, c)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM market_data.public_trades WHERE topic=$1`, topic).Scan(&count); err != nil || count != 0 {
		t.Fatalf("test should have NO public trades: count=%d err=%v", count, err)
	}
}
