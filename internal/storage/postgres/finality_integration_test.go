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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Executes only inside a rollback-only transaction; no shared fixture is
// modified. Requires migration 000005.
func TestSourceEventWatermarkFinalizesAndRejectsTooLate(t *testing.T) {
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires PostgreSQL and migration 000005")
	}
	db, e := sql.Open("pgx", dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	topic := fmt.Sprintf("finality-%d", time.Now().UnixNano())
	start := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	candle := &md.Candle{MarketId: "BTC-USDT", Interval: md.CandleInterval_CANDLE_INTERVAL_ONE_MINUTE,
		StartTime: timestamppb.New(start), EndTime: timestamppb.New(end), OpenPriceUnits: 100, HighPriceUnits: 100, LowPriceUnits: 100, ClosePriceUnits: 100,
		BaseVolumeAtoms: "1", QuoteVolumeAtoms: "1", TradeCount: 1, Revision: 0, IsFinal: false}
	payload, e := proto.Marshal(candle)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO market_data.candles(topic,source_market_id,market_id,interval_id,start_ns,end_ns,revision,payload,is_final)
 VALUES($1,7,'BTC-USDT',1,$2,$3,0,$4,FALSE)`, topic, NanosDecimal(start), NanosDecimal(end), payload); e != nil {
		t.Fatal(e)
	}
	// Even though wall time is much later, the first source event is too early
	// to finalize this candle. Source-time, not now(), drives the cutoff.
	if e = advanceWatermark(ctx, tx, topic, 7, end.Add(2*time.Hour), 10); e != nil {
		t.Fatal(e)
	}
	var isFinal bool
	if e = tx.QueryRowContext(ctx, `SELECT is_final FROM market_data.candles WHERE topic=$1 AND source_market_id=7`, topic).Scan(&isFinal); e != nil || isFinal {
		t.Fatalf("premature finality: %v %v", isFinal, e)
	}
	if e = advanceWatermark(ctx, tx, topic, 7, end.Add(49*time.Hour), 11); e != nil {
		t.Fatal(e)
	}
	var revision int64
	var changed []byte
	if e = tx.QueryRowContext(ctx, `SELECT is_final,revision,payload FROM market_data.candles WHERE topic=$1 AND source_market_id=7`, topic).Scan(&isFinal, &revision, &changed); e != nil {
		t.Fatal(e)
	}
	final := new(md.Candle)
	if e = proto.Unmarshal(changed, final); e != nil {
		t.Fatal(e)
	}
	if !isFinal || revision != 1 || !final.GetIsFinal() || final.GetRevision() != 1 {
		t.Fatalf("finality mismatch: %v %d %v", isFinal, revision, final)
	}
	var journal []byte
	if e = tx.QueryRowContext(ctx, `SELECT payload FROM market_data.candle_updates WHERE topic=$1 AND source_market_id=7 AND market_sequence=11 AND interval_id=1`, topic).Scan(&journal); e != nil {
		t.Fatal(e)
	}
	if string(journal) != string(changed) {
		t.Fatal("finalization not recorded in durable revision journal")
	}
	if e = advanceWatermark(ctx, tx, topic, 7, start.Add(2*time.Second), 12); e == nil {
		t.Fatal("accepted event older than durable watermark")
	}
}
