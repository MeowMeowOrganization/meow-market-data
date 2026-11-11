// Package postgres persists full per-market book checkpoints and Kafka cursors
// atomically, using fenced PostgreSQL transactions. This is non-financial
// projection state, in a separate DB/schema from the accounting ledger.
package postgres

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	matching "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	"google.golang.org/protobuf/proto"

	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	ErrFenced  = errors.New("postgres checkpoint: consumer ownership fenced")
	ErrCursor  = errors.New("postgres checkpoint: cursor compare-and-swap conflict")
	ErrCorrupt = errors.New("postgres checkpoint: corrupted durable snapshot")
)

type Store struct{ DB *sql.DB }

func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("postgres checkpoint: nil DB")
	}
	return &Store{DB: db}, nil
}

func (s *Store) Acquire(ctx context.Context, part recovery.Partition) (recovery.Assignment, error) {
	var a recovery.Assignment
	if part.Topic == "" || part.ID < 0 {
		return a, errors.New("postgres checkpoint: invalid partition")
	}
	var tokenBytes [16]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return a, err
	}
	token := hex.EncodeToString(tokenBytes[:])
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
        INSERT INTO market_data.partition_cursors(topic,partition_id,next_offset)
        VALUES ($1,$2,0) ON CONFLICT DO NOTHING`, part.Topic, part.ID); err != nil {
		return a, err
	}
	if err = tx.QueryRowContext(ctx, `
        UPDATE market_data.partition_cursors
        SET owner_token=$3, fence_generation=fence_generation+1, updated_at=now()
        WHERE topic=$1 AND partition_id=$2
        RETURNING next_offset`, part.Topic, part.ID, token).Scan(&a.NextOffset); err != nil {
		return a, err
	}
	a.Checkpoints = make(map[uint32][]byte)
	rows, err := tx.QueryContext(ctx, `
        SELECT source_market_id, checkpoint, checkpoint_sha256
        FROM market_data.book_checkpoints
        WHERE topic=$1 AND partition_id=$2`, part.Topic, part.ID)
	if err != nil {
		return a, err
	}
	for rows.Next() {
		var id int64
		var blob, digest []byte
		if err = rows.Scan(&id, &blob, &digest); err != nil {
			break
		}
		if id < 0 || id > math.MaxUint32 {
			err = ErrCorrupt
			break
		}
		sha := sha256.Sum256(blob)
		if !bytes.Equal(sha[:], digest) {
			err = fmt.Errorf("%w: market=%d", ErrCorrupt, id)
			break
		}
		a.Checkpoints[uint32(id)] = blob
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return recovery.Assignment{}, err
	}
	if err = tx.Commit(); err != nil {
		return recovery.Assignment{}, err
	}
	a.FenceToken = token
	return a, nil
}

// ProvenanceDigest fetches the immutable accepted envelope digest. Missing
// history is NOT accepted as an idempotent duplicate (requires audited replay).
func (s *Store) ProvenanceDigest(ctx context.Context, part recovery.Partition, marketID uint32, sequence uint64) ([]byte, error) {
	var digest []byte
	err := s.DB.QueryRowContext(ctx, `SELECT payload_sha256 FROM market_data.matching_result_provenance WHERE topic=$1 AND source_market_id=$2 AND market_sequence=$3`, part.Topic, int64(marketID), fmt.Sprint(sequence)).Scan(&digest)
	return digest, err
}

// Save is one PostgreSQL TX per matching result, with a row-lock and
// (owner token, next offset) compare-and-swap precondition. A stale owner and
// an offset hole cannot overwrite another worker's checkpoint.
func (s *Store) Save(ctx context.Context, part recovery.Partition, token string, expectedOffset int64, projection recovery.DurableProjection) error {
	marketID, blob := projection.MarketID, projection.Checkpoint
	if part.Topic == "" || part.ID < 0 || token == "" || expectedOffset < 0 || expectedOffset == math.MaxInt64 || len(blob) == 0 {
		return errors.New("postgres checkpoint: invalid save arguments")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var actualToken sql.NullString
	var current int64
	if err = tx.QueryRowContext(ctx, `
        SELECT owner_token,next_offset FROM market_data.partition_cursors
        WHERE topic=$1 AND partition_id=$2 FOR UPDATE`, part.Topic, part.ID).Scan(&actualToken, &current); err != nil {
		return err
	}
	if !actualToken.Valid || actualToken.String != token {
		return fmt.Errorf("%w: topic=%s partition=%d", ErrFenced, part.Topic, part.ID)
	}
	if current != expectedOffset {
		return fmt.Errorf("%w: expected=%d persisted=%d", ErrCursor, expectedOffset, current)
	}
	if projection.HistoricalDuplicate {
		if projection.Delta != nil || len(projection.Trades) != 0 || projection.Snapshot == nil || projection.Snapshot.GetSourceMarketId() != marketID {
			return errors.New("postgres: malformed historical duplicate")
		}
		var envelope matching.CommittedMatchingResult
		if err := proto.Unmarshal(projection.Envelope, &envelope); err != nil {
			return err
		}
		if envelope.GetResult() == nil || envelope.GetResult().GetCommand() == nil || envelope.GetResult().GetCommand().GetMarketId() != marketID || envelope.GetResult().GetMarketSequence() >= projection.Snapshot.GetMarketSequence() {
			return errors.New("postgres: invalid historical sequence")
		}
		canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&envelope)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(canonical)
		var known []byte
		if err = tx.QueryRowContext(ctx, `SELECT payload_sha256 FROM market_data.matching_result_provenance WHERE topic=$1 AND source_market_id=$2 AND market_sequence=$3`, part.Topic, int64(marketID), fmt.Sprint(envelope.GetResult().GetMarketSequence())).Scan(&known); err != nil {
			return err
		}
		if !bytes.Equal(digest[:], known) {
			return errors.New("postgres: historical duplicate digest conflict")
		}
		var oldSHA []byte
		if err = tx.QueryRowContext(ctx, `SELECT checkpoint_sha256 FROM market_data.book_checkpoints WHERE topic=$1 AND partition_id=$2 AND source_market_id=$3`, part.Topic, part.ID, int64(marketID)).Scan(&oldSHA); err != nil {
			return err
		}
		checkpointSHA := sha256.Sum256(blob)
		if !bytes.Equal(checkpointSHA[:], oldSHA) {
			return errors.New("postgres: historical duplicate would modify checkpoint")
		}
		if _, err = tx.ExecContext(ctx, `UPDATE market_data.partition_cursors SET next_offset=$3, updated_at=now() WHERE topic=$1 AND partition_id=$2`, part.Topic, part.ID, expectedOffset+1); err != nil {
			return err
		}
		return tx.Commit()
	}
	sha := sha256.Sum256(blob)
	if _, err = tx.ExecContext(ctx, `
        INSERT INTO market_data.book_checkpoints
            (topic,partition_id,source_market_id,checkpoint,checkpoint_sha256)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (topic,partition_id,source_market_id)
        DO UPDATE SET checkpoint=EXCLUDED.checkpoint,
                      checkpoint_sha256=EXCLUDED.checkpoint_sha256, updated_at=now()`,
		part.Topic, part.ID, int64(marketID), blob, sha[:]); err != nil {
		return err
	}
	if err = s.savePublicProjection(ctx, tx, part.Topic, projection); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `
        UPDATE market_data.partition_cursors
        SET next_offset=$3,updated_at=now()
        WHERE topic=$1 AND partition_id=$2`, part.Topic, part.ID, expectedOffset+1); err != nil {
		return err
	}
	return tx.Commit()
}

// Release is best-effort. New owners acquire by replacing the token, so a
// crashed owner does not prevent failover. A lost owner cannot clear a newer
// owner's token.
func (s *Store) Release(ctx context.Context, part recovery.Partition, token string) error {
	if token == "" {
		return nil
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE market_data.partition_cursors
        SET owner_token=NULL,updated_at=now()
        WHERE topic=$1 AND partition_id=$2 AND owner_token=$3`, part.Topic, part.ID, token)
	return err
}
