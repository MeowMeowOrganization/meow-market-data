package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
)

type fakeSource struct {
	snapshotSeq uint64
	floor       uint64
	committed   []Result
}

func (f *fakeSource) Snapshot(_ context.Context, m marketregistry.Market) (*md.BookSnapshot, error) {
	return &md.BookSnapshot{MarketId: m.Symbol, SourceMarketId: m.SourceID, MarketSequence: f.snapshotSeq,
		Depth: m.Depth, MarketDefinitionVersion: m.DefinitionVersion}, nil
}
func (f *fakeSource) ReplayFloor(context.Context, marketregistry.Market) (uint64, error) {
	if f.floor > 0 {
		return f.floor, nil
	}
	return 1, nil
}
func (f *fakeSource) Results(_ context.Context, _ marketregistry.Market, after, through uint64, limit int) ([]Result, error) {
	var result []Result
	for _, v := range f.committed {
		seq := v.Delta.GetMarketSequence()
		if seq > after && (through == 0 || seq <= through) {
			result = append(result, v)
		}
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}
func (f *fakeSource) Current(context.Context, marketregistry.Market, time.Time) (*Soft, error) {
	return &Soft{}, nil
}
func mkMarket() marketregistry.Market {
	return marketregistry.Market{Symbol: "BTC-USDT", SourceID: 7, DefinitionVersion: 3, Depth: 20}
}
func mkResult(seq uint64) Result {
	return Result{Delta: &md.BookDelta{
		MarketId: "BTC-USDT", SourceMarketId: 7, MarketSequence: seq, PreviousMarketSequence: seq - 1, Depth: 20, MarketDefinitionVersion: 3,
	}}
}
func TestHubReplaysSequentialCommittedResults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &fakeSource{snapshotSeq: 1, committed: []Result{mkResult(2), mkResult(3)}}
	hub, err := NewHub(ctx, source, 50*time.Millisecond, time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := hub.Subscribe(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for seq := uint64(2); seq <= 3; seq++ {
		select {
		case m := <-sub.Messages:
			if m.Result == nil || m.Result.Delta.GetMarketSequence() != seq {
				t.Fatalf("expected sequence=%d, got %+v", seq, m)
			}
		case e := <-sub.Errors:
			t.Fatal(e)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for committed book stream")
		}
	}
}
func TestHubFailsClosedOnSequenceGap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &fakeSource{snapshotSeq: 1, committed: []Result{mkResult(3)}}
	hub, _ := NewHub(ctx, source, 50*time.Millisecond, time.Second, 2)
	sub, err := hub.Subscribe(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	select {
	case e := <-sub.Errors:
		if !errors.Is(e, ErrSequenceGap) {
			t.Fatalf("got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sequence gap did not close hub")
	}
}
func TestHubBackpressureNeverSilentlyDrops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &fakeSource{snapshotSeq: 1, committed: []Result{mkResult(2), mkResult(3), mkResult(4)}}
	hub, _ := NewHub(ctx, source, 50*time.Millisecond, time.Second, 1)
	sub, err := hub.Subscribe(ctx, mkMarket())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	// Deliberately do not receive from the queue; hub must fail closed.
	select {
	case e := <-sub.Errors:
		if !errors.Is(e, ErrSlowConsumer) {
			t.Fatalf("got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow consumer was not disconnected")
	}
}
