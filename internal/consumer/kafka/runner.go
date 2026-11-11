// Package kafka implements a fenced, manually acknowledged Kafka group
// consumer for complete timestamped committed matching results.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Monitor  *SourceMonitor
	Brokers  []string
	Topic    string
	Group    string
	ClientID string
}

func Run(ctx context.Context, cfg Config, worker *recovery.Worker) error {
	if worker == nil || cfg.Topic == "" || cfg.Group == "" || len(cfg.Brokers) == 0 {
		return errors.New("kafka: worker, brokers, topic and group required")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var fatalMu sync.Mutex
	var fatalErr error
	fail := func(err error) {
		fatalMu.Lock()
		if fatalErr == nil {
			fatalErr = err
		}
		fatalMu.Unlock()
		cancel()
	}
	failure := func() error {
		fatalMu.Lock()
		defer fatalMu.Unlock()
		return fatalErr
	}
	// Franz-go callback lifecycles are serialized with record processing via
	// BlockRebalanceOnPoll. PostgreSQL fence tokens protect against stale
	// consumers even when a broker declares a previous member dead.
	acquire := func(_ context.Context, _ *kgo.Client, assignments map[string][]int32) {
		for topic, parts := range assignments {
			for _, id := range parts {
				if cfg.Monitor != nil {
					cfg.Monitor.assigned(id)
				}
				if err := worker.Assign(ctx, recovery.Partition{Topic: topic, ID: id}); err != nil {
					fail(fmt.Errorf("kafka assignment: %w", err))
					return
				}
			}
		}
	}
	release := func(_ context.Context, _ *kgo.Client, assignments map[string][]int32) {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		for topic, parts := range assignments {
			for _, id := range parts {
				if cfg.Monitor != nil {
					cfg.Monitor.revoked(id)
				}
				if err := worker.Revoke(cleanup, recovery.Partition{Topic: topic, ID: id}); err != nil {
					fail(fmt.Errorf("kafka revocation: %w", err))
					return
				}
			}
		}
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.OnPartitionsAssigned(acquire),
		kgo.OnPartitionsRevoked(release),
		kgo.OnPartitionsLost(release),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer client.CloseAllowingRebalance()
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("kafka ping: %w", err)
	}

	for ctx.Err() == nil {
		// One record per poll is deliberate in this first correctness-oriented
		// milestone: no long processing can block a group rebalance for a large
		// batch. Increase only after throughput/rebalance testing.
		fetches := client.PollRecords(ctx, 1)
		if cfg.Monitor != nil {
			observed := time.Now().UTC()
			fetches.EachPartition(func(part kgo.FetchTopicPartition) {
				if part.Topic == cfg.Topic && part.Err == nil {
					cfg.Monitor.sampled(part.Partition, part.LastStableOffset, observed)
				}
			})
		}
		var processErr error
		fetches.EachError(func(topic string, partition int32, err error) {
			if processErr == nil && ctx.Err() == nil {
				processErr = fmt.Errorf("kafka fetch %s/%d: %w", topic, partition, err)
			}
		})
		if processErr == nil {
			fetches.EachRecord(func(rec *kgo.Record) {
				if processErr != nil {
					return
				}
				processCtx, processDone := context.WithTimeout(ctx, 10*time.Second)
				_, err := worker.Process(processCtx, recovery.Record{
					Partition: recovery.Partition{Topic: rec.Topic, ID: rec.Partition},
					Offset:    rec.Offset, Key: rec.Key, Value: rec.Value,
				})
				processDone()
				if err != nil {
					processErr = err
					return
				}
				// Durable checkpoint is committed BEFORE acknowledging the source.
				// Replays below the PG cursor are also safe to acknowledge.
				commitCtx, commitDone := context.WithTimeout(ctx, 10*time.Second)
				err = client.CommitRecords(commitCtx, rec)
				commitDone()
				if err != nil {
					processErr = fmt.Errorf("kafka commit after durable checkpoint: %w", err)
				}
			})
		}
		client.AllowRebalance() // including errors/shutdown, otherwise close can hang
		if processErr != nil {
			return processErr
		}
	}
	if err := failure(); err != nil {
		return err
	}
	return ctx.Err() // normal signal cancellation is handled by main
}
