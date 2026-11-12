// Package stream distributes durable matching-sequence updates to WebSocket
// consumers. It never uses Redis Pub/Sub as a source of replay authority.
package stream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bookcheck"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	rediscache "github.com/MeowMeowOrganization/meow-market-data/internal/storage/redis"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var ErrSequenceGap = errors.New("stream: durable book delta sequence gap")
var ErrNotReady = readmodel.ErrNotReady

// Result is one *complete* committed matching sequence. Trade and candle
// messages must be delivered before its sequence_end control frame.
type Result struct {
	Delta   *md.BookDelta
	Trades  []*md.TradeTick
	Candles []*md.Candle
}

type Soft struct {
	Ticker *md.Ticker
	Stats  *md.Stats24H
}

type Source interface {
	Snapshot(context.Context, marketregistry.Market) (*md.BookSnapshot, error)
	Head(context.Context, marketregistry.Market) (uint64, error)
	ReplayFloor(context.Context, marketregistry.Market) (uint64, error)
	Results(context.Context, marketregistry.Market, uint64, uint64, int) ([]Result, error)
	Current(context.Context, marketregistry.Market, time.Time) (*Soft, error)
}

type PostgresSource struct {
	DB             *sql.DB
	Topic          string
	Cache          *redis.Client // optional read-only fast path for periodic soft updates
	CacheNamespace rediscache.Namespace
}

func (s *PostgresSource) check() error {
	if s == nil || s.DB == nil || s.Topic == "" {
		return errors.New("stream: missing durable reader")
	}
	return nil
}
func (s *PostgresSource) Snapshot(ctx context.Context, m marketregistry.Market) (*md.BookSnapshot, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	var b []byte
	err := s.DB.QueryRowContext(ctx, `SELECT CASE WHEN octet_length(snapshot) <= $3 THEN snapshot ELSE NULL END
        FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2`, s.Topic, int64(m.SourceID), MaxSnapshotBytes).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotReady
	}
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, ErrPayloadTooLarge
	}
	snap := new(md.BookSnapshot)
	if err := proto.Unmarshal(b, snap); err != nil {
		return nil, fmt.Errorf("stream: malformed snapshot: %w", err)
	}
	if snap.GetMarketId() != m.Symbol || snap.GetSourceMarketId() != m.SourceID || snap.GetDepth() != m.Depth || snap.GetMarketDefinitionVersion() != m.DefinitionVersion || snap.GetMarketSequence() == 0 {
		return nil, errors.New("stream: inconsistent durable snapshot")
	}
	return snap, nil
}

// Head reads only the authoritative durable book cursor. A cache-derived
// cursor is not safe for detecting divergence or estimating live lag.
func (s *PostgresSource) Head(ctx context.Context, m marketregistry.Market) (uint64, error) {
	if err := s.check(); err != nil {
		return 0, err
	}
	var raw string
	err := s.DB.QueryRowContext(ctx, `SELECT market_sequence::text FROM market_data.public_books
        WHERE topic=$1 AND source_market_id=$2`, s.Topic, int64(m.SourceID)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotReady
	}
	if err != nil {
		return 0, err
	}
	seq, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || seq == 0 {
		return 0, ErrSequenceGap
	}
	return seq, nil
}

func (s *PostgresSource) ReplayFloor(ctx context.Context, m marketregistry.Market) (uint64, error) {
	if err := s.check(); err != nil {
		return 0, err
	}
	var decimal string
	err := s.DB.QueryRowContext(ctx, `SELECT first_complete_sequence::text FROM market_data.stream_replay_floors WHERE topic=$1 AND source_market_id=$2`, s.Topic, int64(m.SourceID)).Scan(&decimal)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	} // new market, full history with 000003 installed
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(decimal, 10, 64)
	if err != nil || n == 0 {
		return 0, errors.New("stream: corrupt replay floor")
	}
	return n, nil
}

// Results reads one transactionally consistent, bounded prefix of the durable
// journal. All three queries share one PG REPEATABLE READ snapshot. through=0
// means no upper bound. The owner must assert continuous result sequences.
func (s *PostgresSource) Results(ctx context.Context, m marketregistry.Market, after, through uint64, limit int) ([]Result, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 512 {
		return nil, errors.New("stream: invalid result batch limit")
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Compare as NUMERIC, not int64, to preserve the whole uint64 domain.
	lower, upper := strconv.FormatUint(after, 10), strconv.FormatUint(through, 10)
	rows, err := tx.QueryContext(ctx, `SELECT market_sequence::text,CASE WHEN octet_length(payload) <= $6 THEN payload ELSE NULL END FROM market_data.book_deltas
        WHERE topic=$1 AND source_market_id=$2 AND market_sequence>$3
          AND ($4::numeric=0 OR market_sequence<=$4)
        ORDER BY market_sequence ASC LIMIT $5`, s.Topic, int64(m.SourceID), lower, upper, limit, MaxFeedBytes)
	if err != nil {
		return nil, err
	}
	var out []Result
	var batchBytes int64
	for rows.Next() {
		var seqString string
		var raw []byte
		if err = rows.Scan(&seqString, &raw); err != nil {
			break
		}
		if raw == nil {
			err = ErrPayloadTooLarge
			break
		}
		batchBytes += int64(len(raw))
		if batchBytes > MaxJournalBatchBytes {
			err = ErrBatchTooLarge
			break
		}
		seq, parseErr := strconv.ParseUint(seqString, 10, 64)
		if parseErr != nil {
			err = parseErr
			break
		}
		d := new(md.BookDelta)
		if err = proto.Unmarshal(raw, d); err != nil {
			break
		}
		if d.GetMarketId() != m.Symbol || d.GetSourceMarketId() != m.SourceID || d.GetMarketDefinitionVersion() != m.DefinitionVersion ||
			d.GetDepth() != m.Depth || d.GetMarketSequence() != seq || seq <= after || seq-1 != d.GetPreviousMarketSequence() {
			err = errors.New("stream: corrupt book delta")
			break
		}
		if len(out) == 0 {
			if seq != after+1 {
				err = ErrSequenceGap
				break
			}
		} else if seq != out[len(out)-1].Delta.GetMarketSequence()+1 {
			err = ErrSequenceGap
			break
		}
		out = append(out, Result{Delta: d})
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	highest := strconv.FormatUint(out[len(out)-1].Delta.GetMarketSequence(), 10)
	low := strconv.FormatUint(out[0].Delta.GetMarketSequence(), 10)
	bySeq := make(map[uint64]int, len(out))
	for i, v := range out {
		bySeq[v.Delta.GetMarketSequence()] = i
	}
	trades, err := tx.QueryContext(ctx, `SELECT market_sequence::text,CASE WHEN octet_length(payload) <= $5 THEN payload ELSE NULL END FROM market_data.public_trades
        WHERE topic=$1 AND source_market_id=$2 AND market_sequence >= $3 AND market_sequence <= $4
        ORDER BY market_sequence,event_ordinal LIMIT $6`, s.Topic, int64(m.SourceID), low, highest, MaxFeedBytes, MaxJournalRows+1)
	if err != nil {
		return nil, err
	}
	var tradeRows int
	for trades.Next() {
		tradeRows++
		if tradeRows > MaxJournalRows {
			err = ErrBatchTooLarge
			break
		}
		var seqString string
		var b []byte
		if err = trades.Scan(&seqString, &b); err != nil {
			break
		}
		if b == nil {
			err = ErrPayloadTooLarge
			break
		}
		batchBytes += int64(len(b))
		if batchBytes > MaxJournalBatchBytes {
			err = ErrBatchTooLarge
			break
		}
		seq, parseErr := strconv.ParseUint(seqString, 10, 64)
		if parseErr != nil {
			err = parseErr
			break
		}
		tr := new(md.TradeTick)
		if err = proto.Unmarshal(b, tr); err != nil {
			break
		}
		if tr.GetMatchSequence() != seq || tr.GetSourceMarketId() != m.SourceID || tr.GetMarketId() != m.Symbol {
			err = errors.New("stream: invalid trade provenance")
			break
		}
		idx, ok := bySeq[seq]
		if !ok {
			err = errors.New("stream: trade without durable book delta")
			break
		}
		out[idx].Trades = append(out[idx].Trades, tr)
		if _, err = resultBytes(out[idx]); err != nil {
			break
		}
	}
	if err == nil {
		err = trades.Err()
	}
	trades.Close()
	if err != nil {
		return nil, err
	}
	candles, err := tx.QueryContext(ctx, `SELECT market_sequence::text,CASE WHEN octet_length(payload) <= $5 THEN payload ELSE NULL END FROM market_data.candle_updates
        WHERE topic=$1 AND source_market_id=$2 AND market_sequence >= $3 AND market_sequence <= $4
        ORDER BY market_sequence,interval_id,start_ns LIMIT $6`, s.Topic, int64(m.SourceID), low, highest, MaxFeedBytes, MaxJournalRows+1)
	if err != nil {
		return nil, err
	}
	var candleRows int
	for candles.Next() {
		candleRows++
		if candleRows > MaxJournalRows {
			err = ErrBatchTooLarge
			break
		}
		var seqString string
		var b []byte
		if err = candles.Scan(&seqString, &b); err != nil {
			break
		}
		if b == nil {
			err = ErrPayloadTooLarge
			break
		}
		batchBytes += int64(len(b))
		if batchBytes > MaxJournalBatchBytes {
			err = ErrBatchTooLarge
			break
		}
		seq, parseErr := strconv.ParseUint(seqString, 10, 64)
		if parseErr != nil {
			err = parseErr
			break
		}
		c := new(md.Candle)
		if err = proto.Unmarshal(b, c); err != nil {
			break
		}
		if c.GetMarketId() != m.Symbol {
			err = errors.New("stream: invalid candle market")
			break
		}
		idx, ok := bySeq[seq]
		if !ok {
			err = errors.New("stream: candle without durable book delta")
			break
		}
		out[idx].Candles = append(out[idx].Candles, c)
		if _, err = resultBytes(out[idx]); err != nil {
			break
		}
	}
	if err == nil {
		err = candles.Err()
	}
	candles.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}
func (s *PostgresSource) Current(ctx context.Context, m marketregistry.Market, at time.Time) (*Soft, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	if s.Cache != nil {
		cacheCtx, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		view, err := rediscache.ReadFresh(cacheCtx, s.Cache, s.CacheNamespace, m.SourceID, m.DefinitionVersion, at, "ticker", "stats:24h", "book:top")
		stop()
		if err == nil {
			ticker, stats, book := new(md.Ticker), new(md.Stats24H), new(md.BookSnapshot)
			if protojson.Unmarshal([]byte(view.Values[0]), ticker) == nil && protojson.Unmarshal([]byte(view.Values[1]), stats) == nil &&
				protojson.Unmarshal([]byte(view.Values[2]), book) == nil &&
				bookcheck.Validate(book, bookcheck.Expected{SourceMarketID: m.SourceID, MarketID: m.Symbol,
					DefinitionVersion: m.DefinitionVersion, Depth: m.Depth, PriceTickUnits: m.PriceTickUnits,
					QuantityStepBaseAtoms: m.QuantityStepAtoms}) == nil &&
				book.GetMarketSequence() == view.Sequence && bookcheck.ValidateTicker(ticker, book) == nil &&
				bookcheck.ValidateStats(stats, m.Symbol, at) == nil {
				return &Soft{Ticker: ticker, Stats: stats}, nil
			}
		}
	}
	bundle, err := (&readmodel.Reader{DB: s.DB, Topic: s.Topic}).Load(ctx, m.SourceID, at)
	if err != nil {
		return nil, err
	}
	return &Soft{Ticker: bundle.Ticker, Stats: bundle.Stats}, nil
}
