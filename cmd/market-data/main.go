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
	"github.com/MeowMeowOrganization/meow-market-data/internal/livebook"
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
	var hasJournal, hasFloor, hasMinuteRollups, hasProvenance, hasWatermarks bool
	if err := db.QueryRowContext(ctx, `SELECT
        to_regclass('market_data.candle_updates') IS NOT NULL,
        to_regclass('market_data.stream_replay_floors') IS NOT NULL,
        to_regclass('market_data.trade_minute_rollups') IS NOT NULL,
        to_regclass('market_data.matching_result_provenance') IS NOT NULL,
        to_regclass('market_data.market_event_watermarks') IS NOT NULL`).Scan(&hasJournal, &hasFloor, &hasMinuteRollups, &hasProvenance, &hasWatermarks); err != nil {
		return err
	}
	if !hasJournal || !hasFloor {
		return errors.New("CEX23.6 requires migration 000003_stream_candles.sql")
	}
	if !hasMinuteRollups {
		return errors.New("CEX24.3 requires migration 000004_trade_minute_rollups.sql (stop consumer and backfill first)")
	}
	if !hasProvenance || !hasWatermarks {
		return errors.New("CEX25.2 requires migration 000005_cex25_finality_provenance.sql")
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
	cache, err := rediscache.New(cfg.RedisURL, cfg.Topic, cfg.RedisProjectionEpoch)
	if err != nil {
		return err
	}
	defer cache.Close()

	source := &stream.PostgresSource{DB: db, Topic: cfg.Topic, Cache: cache.Client, CacheNamespace: cache.Namespace}
	hub, err := stream.NewHub(runCtx, source, cfg.StreamPollInterval, cfg.StreamSoftInterval, 256)
	if err != nil {
		return err
	}
	wsHandler, err := stream.NewHandler(registry, source, hub, cfg.AllowedOrigins, cfg.MaxWSConnections)
	if err != nil {
		return err
	}
	wsHandler.Shutdown = runCtx.Done()
	rpc, err := publicapi.NewService(registry, &publicapi.Repository{DB: db, Topic: cfg.Topic, Cache: cache.Client, CacheNamespace: cache.Namespace}, []byte(cfg.CursorSecret))
	if err != nil {
		return err
	}
	bookRPC, err := stream.NewBookRPC(registry, source, hub, cfg.MaxBookRPCStreams)
	if err != nil {
		return err
	}
	rpc.BookStream = bookRPC
	monitor := kafka.NewSourceMonitor(db, cfg.Topic)
	mux := http.NewServeMux()
	prefix, connectHandler := apiv1alpha1connect.NewMarketDataServiceHandler(rpc, connect.WithReadMaxBytes(64<<10), connect.WithSendMaxBytes(4<<20))
	// The response controller must refer to the ORIGINAL HTTP ResponseWriter;
	// attach it before Connect's internal handler wrappers run.
	mux.Handle(prefix, stream.WithBookWriteDeadlines(connectHandler))
	mux.Handle("/v1/market-data/stream", wsHandler)
	mux.HandleFunc("/source-healthz", monitor.Handler)
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
	markets := make([]marketregistry.Market, 0, len(cfg.Markets))
	for _, m := range cfg.Markets {
		ids = append(ids, m.Definition.GetSourceMarketId())
		market, ok := registry.ByID(m.Definition.GetSourceMarketId())
		if !ok {
			return errors.New("livebook: configured market missing from registry")
		}
		markets = append(markets, market)
	}
	go func() {
		_ = readmodel.Run(runCtx, &readmodel.Reader{DB: db, Topic: cfg.Topic}, cache, ids, cfg.RedisRefreshInterval, logger)
	}()
	go func() {
		projector := &livebook.Worker{
			Journal: &livebook.PostgresJournal{DB: db, Topic: cfg.Topic},
			Cache:   livebook.RedisAdapter{Cache: cache}, Markets: markets,
		}
		_ = projector.Run(runCtx, cfg.RedisBookPollInterval, logger)
	}()
	logger.Info("market-data started", "http", cfg.HTTPAddr, "topic", cfg.Topic, "group", cfg.Group, "markets", len(ids))
	err = kafka.Run(runCtx, kafka.Config{Brokers: cfg.Brokers, Topic: cfg.Topic, Group: cfg.Group, ClientID: cfg.ClientID, Monitor: monitor}, worker)
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
