package livebook

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bookcheck"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/projection/l2"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"google.golang.org/protobuf/proto"
)

var ErrJournalGap = errors.New("livebook: durable journal gap")
var ErrCorruptJournal = errors.New("livebook: corrupt durable journal entry")
var ErrSourceRegression = errors.New("livebook: Redis sequence ahead of durable source")

// Journal is solely committed public projection data. It does not poll Kafka
// or participate in the matching-result consumer transaction.
type Journal interface {
	Snapshot(context.Context, marketregistry.Market) (*md.BookSnapshot, error)
	Fetch(context.Context, marketregistry.Market, uint64, int) (uint64, []*md.BookDelta, error)
}

type PostgresJournal struct {
	DB    *sql.DB
	Topic string
}

func (p *PostgresJournal) check() error {
	if p == nil || p.DB == nil || p.Topic == "" {
		return errors.New("livebook: missing PostgreSQL journal or topic")
	}
	return nil
}
func expected(m marketregistry.Market) bookcheck.Expected {
	return bookcheck.Expected{SourceMarketID: m.SourceID, MarketID: m.Symbol, DefinitionVersion: m.DefinitionVersion,
		Depth: m.Depth, PriceTickUnits: m.PriceTickUnits, QuantityStepBaseAtoms: m.QuantityStepAtoms}
}
func (p *PostgresJournal) Snapshot(ctx context.Context, m marketregistry.Market) (*md.BookSnapshot, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	var payload []byte
	err := p.DB.QueryRowContext(ctx, `SELECT snapshot FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2`, p.Topic, int64(m.SourceID)).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, readmodel.ErrNotReady
	}
	if err != nil {
		return nil, err
	}
	snapshot := new(md.BookSnapshot)
	if err := proto.Unmarshal(payload, snapshot); err != nil {
		return nil, fmt.Errorf("livebook: corrupt durable snapshot: %w", err)
	}
	if err := bookcheck.Validate(snapshot, expected(m)); err != nil {
		return nil, fmt.Errorf("livebook: invalid durable snapshot: %w", err)
	}
	return snapshot, nil
}

// Fetch observes a repeatable-read transaction snapshot of the committed
// journal and its high watermark. When head > cursor but the next contiguous
// row is absent, it reports an explicit gap rather than appearing caught up.
// Queries and numeric parameters preserve the full uint64 domain.
func (p *PostgresJournal) Fetch(ctx context.Context, m marketregistry.Market, after uint64, limit int) (uint64, []*md.BookDelta, error) {
	if err := p.check(); err != nil {
		return 0, nil, err
	}
	if limit < 1 || limit > 512 {
		return 0, nil, errors.New("livebook: invalid journal batch size")
	}
	tx, err := p.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()
	var headRaw, versionRaw string
	var symbol string
	var depth uint32
	err = tx.QueryRowContext(ctx, `SELECT market_sequence::text,market_definition_version::text,market_id,depth
       FROM market_data.public_books WHERE topic=$1 AND source_market_id=$2`, p.Topic, int64(m.SourceID)).Scan(&headRaw, &versionRaw, &symbol, &depth)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, readmodel.ErrNotReady
	}
	if err != nil {
		return 0, nil, err
	}
	head, err := strconv.ParseUint(headRaw, 10, 64)
	if err != nil || head == 0 {
		return 0, nil, errors.New("livebook: invalid durable book head")
	}
	version, err := strconv.ParseUint(versionRaw, 10, 64)
	if err != nil || version != m.DefinitionVersion || symbol != m.Symbol || depth != m.Depth {
		return 0, nil, errors.New("livebook: durable head definition mismatch")
	}
	if head < after {
		return head, nil, ErrSourceRegression
	}
	rows, err := tx.QueryContext(ctx, `SELECT market_sequence::text,payload FROM market_data.book_deltas
       WHERE topic=$1 AND source_market_id=$2 AND market_sequence>$3
       ORDER BY market_sequence ASC LIMIT $4`, p.Topic, int64(m.SourceID), strconv.FormatUint(after, 10), limit)
	if err != nil {
		return 0, nil, err
	}
	var deltas []*md.BookDelta
	current := after
	for rows.Next() {
		var rawSeq string
		var payload []byte
		if err = rows.Scan(&rawSeq, &payload); err != nil {
			break
		}
		seq, parseErr := strconv.ParseUint(rawSeq, 10, 64)
		if parseErr != nil {
			err = fmt.Errorf("%w: sequence: %v", ErrCorruptJournal, parseErr)
			break
		}
		d := new(md.BookDelta)
		if err = proto.Unmarshal(payload, d); err != nil {
			err = fmt.Errorf("%w: delta protobuf: %v", ErrCorruptJournal, err)
			break
		}
		// An absent intermediate journal entry is distinct from an idle market.
		if seq > head || seq <= current || current == ^uint64(0) || seq != current+1 {
			err = ErrJournalGap
			break
		}
		if d.GetMarketSequence() != seq || d.GetPreviousMarketSequence() != current {
			err = ErrCorruptJournal
			break
		}
		if err = l2.ValidateDelta(d, expected(m)); err != nil {
			err = fmt.Errorf("%w: %v", ErrCorruptJournal, err)
			break
		}
		deltas = append(deltas, d)
		current = seq
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return 0, nil, err
	}
	if (len(deltas) == 0 && head > after) || (len(deltas) < limit && current < head) {
		return head, nil, ErrJournalGap
	}
	if err = tx.Commit(); err != nil {
		return 0, nil, err
	}
	return head, deltas, nil
}
