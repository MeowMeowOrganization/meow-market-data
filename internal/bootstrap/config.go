package bootstrap

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	marketdatav1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"google.golang.org/protobuf/encoding/protojson"
)

type Config struct {
	DatabaseURL               string
	Brokers                   []string
	Topic                     string
	Group                     string
	ClientID                  string
	RedisURL                  string
	RedisProjectionEpoch      string
	RedisRefreshInterval      time.Duration
	RedisBookPollInterval     time.Duration
	HTTPAddr                  string
	CursorSecret              string
	AllowedOrigins            []string
	MaxWSConnections          int
	MaxBookRPCStreams         int
	MaxTradeRPCStreams        int
	StreamPollInterval        time.Duration
	StreamSoftInterval        time.Duration
	StreamMaxHeadLagSequences uint64
	StreamMaxHeadLagAge       time.Duration
	StreamMaxPollBatches      int
	Markets                   []recovery.MarketConfig
}

func LoadFromEnvironment() (Config, error) {
	var cfg Config
	cfg.DatabaseURL = os.Getenv("MARKET_DATA_DATABASE_URL")
	brokers := strings.TrimSpace(os.Getenv("MARKET_DATA_KAFKA_BROKERS"))
	for _, b := range strings.Split(brokers, ",") {
		if v := strings.TrimSpace(b); v != "" {
			cfg.Brokers = append(cfg.Brokers, v)
		}
	}
	cfg.Topic = os.Getenv("MARKET_DATA_KAFKA_TOPIC")
	cfg.Group = os.Getenv("MARKET_DATA_KAFKA_GROUP")
	cfg.ClientID = os.Getenv("MARKET_DATA_KAFKA_CLIENT_ID")
	cfg.RedisURL = os.Getenv("MARKET_DATA_REDIS_URL")
	cfg.RedisProjectionEpoch = "1"
	if v := os.Getenv("MARKET_DATA_REDIS_PROJECTION_EPOCH"); v != "" {
		cfg.RedisProjectionEpoch = v
	}
	cfg.RedisRefreshInterval = 5 * time.Second
	cfg.RedisBookPollInterval = 100 * time.Millisecond
	cfg.HTTPAddr = "127.0.0.1:8090"
	if v := os.Getenv("MARKET_DATA_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	cfg.CursorSecret = os.Getenv("MARKET_DATA_CURSOR_SECRET")
	origins := strings.TrimSpace(os.Getenv("MARKET_DATA_PUBLIC_ALLOWED_ORIGINS"))
	for _, v := range strings.Split(origins, ",") {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			cfg.AllowedOrigins = append(cfg.AllowedOrigins, trimmed)
		}
	}
	cfg.MaxWSConnections = 256
	if v := os.Getenv("MARKET_DATA_HTTP_MAX_WS_CONNECTIONS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			return cfg, errors.New("bootstrap: invalid maximum WS connections")
		}
		cfg.MaxWSConnections = n
	}
	cfg.MaxBookRPCStreams = 256
	if v := os.Getenv("MARKET_DATA_HTTP_MAX_BOOK_RPC_STREAMS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			return cfg, errors.New("bootstrap: invalid maximum Connect book streams")
		}
		cfg.MaxBookRPCStreams = n
	}
	cfg.MaxTradeRPCStreams = 256
	if v := os.Getenv("MARKET_DATA_HTTP_MAX_TRADE_RPC_STREAMS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			return cfg, errors.New("bootstrap: invalid maximum Connect trade streams")
		}
		cfg.MaxTradeRPCStreams = n
	}
	cfg.StreamPollInterval = 250 * time.Millisecond
	cfg.StreamSoftInterval = 5 * time.Second
	if v := os.Getenv("MARKET_DATA_STREAM_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 50*time.Millisecond {
			return cfg, errors.New("bootstrap: stream poll interval must be >=50ms")
		}
		cfg.StreamPollInterval = d
	}
	if v := os.Getenv("MARKET_DATA_STREAM_SOFT_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second {
			return cfg, errors.New("bootstrap: stream soft interval must be >=1s")
		}
		cfg.StreamSoftInterval = d
	}
	// Strict, bounded head-lag policy. Both checks use the durable PG head.
	cfg.StreamMaxHeadLagSequences = 2048
	if v := os.Getenv("MARKET_DATA_STREAM_MAX_HEAD_LAG_SEQUENCES"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n == 0 {
			return cfg, errors.New("bootstrap: max head lag sequences must be positive uint64")
		}
		cfg.StreamMaxHeadLagSequences = n
	}
	cfg.StreamMaxHeadLagAge = 3 * time.Second
	if v := os.Getenv("MARKET_DATA_STREAM_MAX_HEAD_LAG_AGE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 100*time.Millisecond {
			return cfg, errors.New("bootstrap: max head lag age must be >=100ms")
		}
		cfg.StreamMaxHeadLagAge = d
	}
	cfg.StreamMaxPollBatches = 4
	if v := os.Getenv("MARKET_DATA_STREAM_MAX_POLL_BATCHES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 32 {
			return cfg, errors.New("bootstrap: max poll batches must be 1..32")
		}
		cfg.StreamMaxPollBatches = n
	}
	if len(cfg.RedisProjectionEpoch) > 128 {
		return cfg, errors.New("bootstrap: Redis projection epoch exceeds 128 bytes")
	}
	if len(cfg.CursorSecret) < 32 {
		return cfg, errors.New("bootstrap: MARKET_DATA_CURSOR_SECRET must contain at least 32 bytes")
	}

	if v := os.Getenv("MARKET_DATA_REDIS_REFRESH_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second {
			return cfg, errors.New("bootstrap: Redis refresh interval must be >= 1s")
		}
		cfg.RedisRefreshInterval = d
	}
	if v := os.Getenv("MARKET_DATA_REDIS_BOOK_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 50*time.Millisecond {
			return cfg, errors.New("bootstrap: Redis book poll interval must be >=50ms")
		}
		cfg.RedisBookPollInterval = d
	}
	path := os.Getenv("MARKET_DATA_MARKETS_FILE")
	if cfg.DatabaseURL == "" || len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.Group == "" || path == "" || cfg.RedisURL == "" {
		return cfg, errors.New("bootstrap: DATABASE_URL, KAFKA_BROKERS, KAFKA_TOPIC, KAFKA_GROUP MARKETS_FILE and REDIS_URL required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("bootstrap: read registry: %w", err)
	}
	var registry struct {
		Markets []struct {
			Partition  int32           `json:"partition"`
			Depth      uint32          `json:"depth"`
			Definition json.RawMessage `json:"definition"`
		} `json:"markets"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		return cfg, err
	}
	if len(registry.Markets) == 0 {
		return cfg, errors.New("bootstrap: empty market registry")
	}
	for _, item := range registry.Markets {
		def := new(marketdatav1.MarketProjectionDefinition)
		if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(item.Definition, def); err != nil {
			return cfg, fmt.Errorf("bootstrap: invalid market definition: %w", err)
		}
		cfg.Markets = append(cfg.Markets, recovery.MarketConfig{PartitionID: item.Partition, Depth: item.Depth, Definition: def})
	}
	return cfg, nil
}
