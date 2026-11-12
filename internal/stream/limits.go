package stream

import (
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
)

// Limits apply to all transports using this hub. In particular, a bounded
// number of Go channel entries is not a bounded number of payload bytes.
const (
	MaxSnapshotBytes     = 1 << 20   // encoded protobuf bytes
	MaxFeedBytes         = 1 << 20   // one matching result, including all trades/candles
	MaxSubscriberBytes   = 4 << 20   // queued (not currently being written) per client
	MaxTotalQueuedBytes  = 128 << 20 // all hub subscribers, across markets
	MaxJournalBatchBytes = 8 << 20   // total encoded journal bytes read per query
	MaxJournalRows       = 8192      // trades or candles read per query (plus sentinel)
	MaxWebSocketBytes    = 1 << 20   // exact encoded JSON frame bytes
)

var (
	ErrPayloadTooLarge = errors.New("stream: durable payload exceeds byte budget")
	ErrBatchTooLarge   = errors.New("stream: journal batch exceeds query byte/row budget")
	ErrStreamStale     = errors.New("stream: projection too far behind durable head; snapshot resync required")
	ErrStreamShutdown  = errors.New("stream: shutting down")
)

type LagPolicy struct {
	// A stalled stream is terminated when EITHER threshold is exceeded. An
	// idle market at its durable head is never stale.
	MaxHeadSequences uint64
	MaxBehind        time.Duration
	MaxPollBatches   int
}

func DefaultLagPolicy() LagPolicy {
	return LagPolicy{MaxHeadSequences: 2048, MaxBehind: 3 * time.Second, MaxPollBatches: 4}
}

func (p LagPolicy) valid() bool {
	return p.MaxHeadSequences > 0 && p.MaxBehind >= 100*time.Millisecond && p.MaxPollBatches >= 1 && p.MaxPollBatches <= 32
}

func resultBytes(r Result) (int64, error) {
	if r.Delta == nil {
		return 0, ErrSequenceGap
	}
	n := int64(proto.Size(r.Delta))
	for _, t := range r.Trades {
		if t == nil {
			return 0, ErrPayloadTooLarge
		}
		n += int64(proto.Size(t))
		if n > MaxFeedBytes {
			return 0, ErrPayloadTooLarge
		}
	}
	for _, c := range r.Candles {
		if c == nil {
			return 0, ErrPayloadTooLarge
		}
		n += int64(proto.Size(c))
		if n > MaxFeedBytes {
			return 0, ErrPayloadTooLarge
		}
	}
	if n > MaxFeedBytes {
		return 0, ErrPayloadTooLarge
	}
	return n, nil
}
