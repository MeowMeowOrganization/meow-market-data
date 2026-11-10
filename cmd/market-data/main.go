package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1/apiv1alpha1connect"
	"github.com/MeowMeowOrganization/meow-market-data/internal/bootstrap"
	"github.com/MeowMeowOrganization/meow-market-data/internal/consumer/kafka"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/publicapi"
	"github.com/MeowMeowOrganization/meow-market-data/internal/readmodel"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"github.com/MeowMeowOrganization/meow-market-data/internal/storage/postgres"
	rediscache "github.com/MeowMeowOrganization/meow-market-data/internal/storage/redis"
	"github.com/MeowMeowOrganization/meow-market-data/internal/stream"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("market data halted; do not acknowledge further source records", "error", err)
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	cfg, err := bootstrap.LoadFromEnvironment()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(16)
	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		cancelPing()
		return err
	}
	cancelPing()
	// Fail at startup instead of checkpointing new matching results without
	// the transactional candle revision journal introduced by CEX23.6.
	var hasJournal, hasFloor bool
	if err := db.QueryRowContext(ctx, `SELECT
        to_regclass('market_data.candle_updates') IS NOT NULL,
        to_regclass('market_data.stream_replay_floors') IS NOT NULL`).Scan(&hasJournal, &hasFloor); err != nil {
		return err
	}
	if !hasJournal || !hasFloor {
		return errors.New("CEX23.6 requires migration 000003_stream_candles.sql")
	}
	store, err := postgres.New(db)
	if err != nil {
		return err
	}
	worker, err := recovery.New(cfg.Topic, store, cfg.Markets)
	if err != nil {
		return err
	}
	registry, err := marketregistry.New(cfg.Markets)
	if err != nil {
		return err
	}
	cache, err := rediscache.New(cfg.RedisURL)
	if err != nil {
		return err
	}
	defer cache.Close()

	source := &stream.PostgresSource{DB: db, Topic: cfg.Topic, Cache: cache.Client}
	hub, err := stream.NewHub(runCtx, source, cfg.StreamPollInterval, cfg.StreamSoftInterval, 256)
	if err != nil {
		return err
	}
	wsHandler, err := stream.NewHandler(registry, source, hub, cfg.AllowedOrigins, cfg.MaxWSConnections)
	if err != nil {
		return err
	}
	wsHandler.Shutdown = runCtx.Done()
	rpc, err := publicapi.NewService(registry, &publicapi.Repository{DB: db, Topic: cfg.Topic, Cache: cache.Client}, []byte(cfg.CursorSecret))
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	prefix, connectHandler := apiv1alpha1connect.NewMarketDataServiceHandler(rpc, connect.WithReadMaxBytes(64<<10), connect.WithSendMaxBytes(4<<20))
	mux.Handle(prefix, connectHandler)
	mux.Handle("/v1/market-data/stream", wsHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		checkCtx, stop := context.WithTimeout(r.Context(), 2*time.Second)
		defer stop()
		if db.PingContext(checkCtx) != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 120 * time.Second}
	httpErrors := make(chan error, 1)
	go func() {
		e := server.Serve(listener)
		if e != nil && !errors.Is(e, http.ErrServerClosed) {
			httpErrors <- e
			cancelRun()
		}
	}()
	defer func() {
		cancelRun() // first, stop WS streams and the Kafka group
		closeCtx, done := context.WithTimeout(context.Background(), 8*time.Second)
		defer done()
		_ = server.Shutdown(closeCtx)
		_ = server.Close()
	}()

	ids := make([]uint32, 0, len(cfg.Markets))
	for _, m := range cfg.Markets {
		ids = append(ids, m.Definition.GetSourceMarketId())
	}
	go func() {
		_ = readmodel.Run(runCtx, &readmodel.Reader{DB: db, Topic: cfg.Topic}, cache, ids, cfg.RedisRefreshInterval, logger)
	}()
	logger.Info("market-data started", "http", cfg.HTTPAddr, "topic", cfg.Topic, "group", cfg.Group, "markets", len(ids))
	err = kafka.Run(runCtx, kafka.Config{Brokers: cfg.Brokers, Topic: cfg.Topic, Group: cfg.Group, ClientID: cfg.ClientID}, worker)
	select {
	case httpErr := <-httpErrors:
		return httpErr
	default:
	}
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}
