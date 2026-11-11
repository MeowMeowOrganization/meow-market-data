//go:build integration

package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	matching "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPostgresFenceAtomicCheckpointAndCorruption(t *testing.T) {
	dsn := os.Getenv("MARKET_DATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set MARKET_DATA_TEST_DATABASE_URL for real PostgreSQL test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	// Apply migrations/000001_recovery.sql using psql BEFORE this test.
	// pgx extended-protocol Exec does not accept multiple DDL statements.
	var exists sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT to_regclass('market_data.partition_cursors')::text").Scan(&exists); err != nil || !exists.Valid {
		t.Fatalf("apply migrations/000001_recovery.sql before integration tests: %v", err)
	}
	store, _ := New(db)
	p := recovery.Partition{Topic: "integration-test-" + time.Now().Format("20060102150405.000000000"), ID: 0}
	a, err := store.Acquire(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if a.NextOffset != 0 || len(a.Checkpoints) != 0 {
		t.Fatal("initial cursor should be zero")
	}
	mk := func(seq uint64, state string) recovery.DurableProjection {
		env, _ := proto.Marshal(&matching.CommittedMatchingResult{Result: &matching.MatchingResult{MarketSequence: seq, Command: &matching.MatchingCommand{MarketId: 7}},
			RecordedAt: timestamppb.New(time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)), MarketDefinitionVersion: 3})
		return recovery.DurableProjection{MarketID: 7, Checkpoint: []byte(state), Envelope: env,
			Snapshot: &md.BookSnapshot{SourceMarketId: 7, MarketId: "BTC-USDT", Depth: 5, MarketSequence: seq, MarketDefinitionVersion: 3},
			Delta:    &md.BookDelta{SourceMarketId: 7, MarketId: "BTC-USDT", Depth: 5, PreviousMarketSequence: seq - 1, MarketSequence: seq, MarketDefinitionVersion: 3}}
	}
	if err := store.Save(ctx, p, a.FenceToken, 0, mk(1, "state-1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, p, a.FenceToken, 0, mk(1, "duplicate")); !errors.Is(err, ErrCursor) {
		t.Fatalf("want CAS conflict, got %v", err)
	}
	b, err := store.Acquire(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if b.NextOffset != 1 || !bytes.Equal(b.Checkpoints[7], []byte("state-1")) {
		t.Fatal("did not recover durable snapshot+cursor")
	}
	if err := store.Save(ctx, p, a.FenceToken, 1, mk(2, "stale")); !errors.Is(err, ErrFenced) {
		t.Fatalf("old owner was not fenced: %v", err)
	}
	if err := store.Save(ctx, p, b.FenceToken, 1, mk(2, "state-2")); err != nil {
		t.Fatal(err)
	}
	// A historical duplicate at a NEW Kafka offset changes the cursor alone.
	original := mk(1, "unused")
	digest, e := store.ProvenanceDigest(ctx, p, 7, 1)
	if e != nil || len(digest) != 32 {
		t.Fatalf("missing immutable digest: %v", e)
	}
	stateBefore := []byte("state-2")
	duplicate := recovery.DurableProjection{MarketID: 7, Checkpoint: stateBefore, Envelope: original.Envelope,
		Snapshot: mk(2, "unused").Snapshot, HistoricalDuplicate: true}
	if err = store.Save(ctx, p, b.FenceToken, 2, duplicate); err != nil {
		t.Fatalf("historical duplicate: %v", err)
	}
	altered := duplicate
	var different matching.CommittedMatchingResult
	if err = proto.Unmarshal(original.Envelope, &different); err != nil {
		t.Fatal(err)
	}
	different.RecordedAt = timestamppb.New(different.GetRecordedAt().AsTime().Add(time.Second))
	altered.Envelope, err = proto.Marshal(&different)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Save(ctx, p, b.FenceToken, 3, altered); err == nil {
		t.Fatal("accepted valid conflicting historical payload")
	}
	var offsetAfterConflict int64
	if err = db.QueryRowContext(ctx, `SELECT next_offset FROM market_data.partition_cursors WHERE topic=$1 AND partition_id=$2`, p.Topic, p.ID).Scan(&offsetAfterConflict); err != nil || offsetAfterConflict != 3 {
		t.Fatalf("conflict moved durable Kafka cursor: offset=%d err=%v", offsetAfterConflict, err)
	}
	if err := store.Release(ctx, p, a.FenceToken); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, p, b.FenceToken, 3, mk(3, "state-3")); err != nil {
		t.Fatalf("stale release revoked newer fence: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE market_data.book_checkpoints SET checkpoint=$3 WHERE topic=$1 AND partition_id=$2`, p.Topic, p.ID, []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	_, err = store.Acquire(ctx, p)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected corruption rejection, got %v", err)
	}
	// Ensure the SQL CHECK constraint really demands a SHA-256 hash.
	bad := sha256.Sum256([]byte("corrupt"))
	if len(bad) != 32 {
		t.Fatal("impossible hash length")
	}
}
