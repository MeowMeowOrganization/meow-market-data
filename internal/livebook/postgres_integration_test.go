//go:build integration

package livebook

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/proto"
)

func TestPostgresJournalContiguityAndGapDetection(t *testing.T) {
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set MARKET_DATA_TEST_DATABASE_URL; apply migration 000002")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	m := testMarket
	m.SourceID = uint32(time.Now().UnixNano())
	topic := fmt.Sprintf("live-journal-test.%d", time.Now().UnixNano())
	const base uint64 = 9007199254740993
	snap := testSnapshot(base + 2)
	snap.SourceMarketId = m.SourceID
	raw, err := proto.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO market_data.public_books
      (topic,source_market_id,market_id,market_sequence,market_definition_version,depth,recorded_at_ns,snapshot)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, topic, int64(m.SourceID), m.Symbol,
		strconv.FormatUint(snap.GetMarketSequence(), 10), "3", m.Depth, postgres.NanosDecimal(time.Now().UTC()), raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM market_data.book_deltas WHERE topic=$1 AND source_market_id=$2`, topic, int64(m.SourceID))
		_, _ = db.ExecContext(context.Background(), `DELETE FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2`, topic, int64(m.SourceID))
	})
	for seq := base + 1; seq <= base+2; seq++ {
		d := testDelta(seq)
		d.SourceMarketId = m.SourceID
		payload, err := proto.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.ExecContext(ctx, `INSERT INTO market_data.book_deltas(topic,source_market_id,market_sequence,payload)
           VALUES($1,$2,$3,$4)`, topic, int64(m.SourceID), strconv.FormatUint(seq, 10), payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	journal := &PostgresJournal{DB: db, Topic: topic}
	got, err := journal.Snapshot(ctx, m)
	if err != nil || !proto.Equal(got, snap) {
		t.Fatalf("durable snapshot: %+v %v", got, err)
	}
	head, ds, err := journal.Fetch(ctx, m, base, 128)
	if err != nil || head != base+2 || len(ds) != 2 || ds[0].GetMarketSequence() != base+1 || ds[1].GetMarketSequence() != base+2 {
		t.Fatalf("contiguous journal: head=%d deltas=%v err=%v", head, ds, err)
	}
	head, ds, err = journal.Fetch(ctx, m, base+2, 128)
	if err != nil || head != base+2 || len(ds) != 0 {
		t.Fatalf("idle journal: head=%d deltas=%v err=%v", head, ds, err)
	}
	_, err = db.ExecContext(ctx, `DELETE FROM market_data.book_deltas WHERE topic=$1 AND source_market_id=$2 AND market_sequence=$3`,
		topic, int64(m.SourceID), strconv.FormatUint(base+1, 10))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = journal.Fetch(ctx, m, base, 128)
	if !errors.Is(err, ErrJournalGap) {
		t.Fatalf("missing first sequence not detected: %v", err)
	}
	_, err = db.ExecContext(ctx, `DELETE FROM market_data.book_deltas WHERE topic=$1 AND source_market_id=$2`, topic, int64(m.SourceID))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = journal.Fetch(ctx, m, base, 128)
	if !errors.Is(err, ErrJournalGap) {
		t.Fatalf("missing all later sequences not detected: %v", err)
	}
	_, _, err = journal.Fetch(ctx, m, base+3, 128)
	if !errors.Is(err, ErrSourceRegression) {
		t.Fatalf("source regression not detected: %v", err)
	}
}
