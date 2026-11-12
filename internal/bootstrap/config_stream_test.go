package bootstrap

import (
	"strings"
	"testing"
	"time"
)

func configureMarketDataTestEnv(t *testing.T) {
	t.Helper()
	for key, value := range map[string]string{
		"MARKET_DATA_DATABASE_URL":  "postgres://local:local@localhost:5432/test",
		"MARKET_DATA_KAFKA_BROKERS": "localhost:9092",
		"MARKET_DATA_KAFKA_TOPIC":   "matching.results.committed.v1",
		"MARKET_DATA_KAFKA_GROUP":   "test",
		"MARKET_DATA_REDIS_URL":     "redis://localhost:6379/0",
		"MARKET_DATA_CURSOR_SECRET": strings.Repeat("x", 32),
		"MARKET_DATA_MARKETS_FILE":  "../../config/markets.example.json",
	} {
		t.Setenv(key, value)
	}
	t.Setenv("MARKET_DATA_STREAM_MAX_HEAD_LAG_SEQUENCES", "")
	t.Setenv("MARKET_DATA_STREAM_MAX_HEAD_LAG_AGE", "")
	t.Setenv("MARKET_DATA_STREAM_MAX_POLL_BATCHES", "")
}
func TestStreamLagConfigurationDefaultsAndOverrides(t *testing.T) {
	configureMarketDataTestEnv(t)
	cfg, err := LoadFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StreamMaxHeadLagSequences != 2048 || cfg.StreamMaxHeadLagAge != 3*time.Second || cfg.StreamMaxPollBatches != 4 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	t.Setenv("MARKET_DATA_STREAM_MAX_HEAD_LAG_SEQUENCES", "100")
	t.Setenv("MARKET_DATA_STREAM_MAX_HEAD_LAG_AGE", "750ms")
	t.Setenv("MARKET_DATA_STREAM_MAX_POLL_BATCHES", "2")
	cfg, err = LoadFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StreamMaxHeadLagSequences != 100 || cfg.StreamMaxHeadLagAge != 750*time.Millisecond || cfg.StreamMaxPollBatches != 2 {
		t.Fatalf("unexpected overrides: %+v", cfg)
	}
}
func TestStreamLagConfigurationRejectsInvalidBudgets(t *testing.T) {
	for _, tc := range []struct{ key, val string }{
		{"MARKET_DATA_STREAM_MAX_HEAD_LAG_SEQUENCES", "0"},
		{"MARKET_DATA_STREAM_MAX_HEAD_LAG_SEQUENCES", "-1"},
		{"MARKET_DATA_STREAM_MAX_HEAD_LAG_AGE", "99ms"},
		{"MARKET_DATA_STREAM_MAX_HEAD_LAG_AGE", "invalid"},
		{"MARKET_DATA_STREAM_MAX_POLL_BATCHES", "0"},
		{"MARKET_DATA_STREAM_MAX_POLL_BATCHES", "33"},
	} {
		t.Run(tc.key+"="+tc.val, func(t *testing.T) {
			configureMarketDataTestEnv(t)
			t.Setenv(tc.key, tc.val)
			if _, err := LoadFromEnvironment(); err == nil {
				t.Fatal("accepted unsafe budget")
			}
		})
	}
}
