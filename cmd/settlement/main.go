// Command settlement runs the bet settlement service.
//
// main only wires dependencies together (composition root) and manages
// the lifecycle: start components, wait for a signal, shut down gracefully.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Big-Lapa/bet-settlement-go/internal/adapter/httpapi"
	"github.com/Big-Lapa/bet-settlement-go/internal/adapter/kafka"
	"github.com/Big-Lapa/bet-settlement-go/internal/adapter/memory"
	"github.com/Big-Lapa/bet-settlement-go/internal/adapter/postgres"
	"github.com/Big-Lapa/bet-settlement-go/internal/app"
	"github.com/Big-Lapa/bet-settlement-go/internal/config"
	"github.com/Big-Lapa/bet-settlement-go/internal/outbox"
)

func main() {
	if err := run(); err != nil {
		slog.Error("service stopped with error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	// ctx is cancelled on SIGINT/SIGTERM (Kubernetes sends SIGTERM on pod stop).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// errgroup: if any component fails, gctx is cancelled and the others stop too.
	g, gctx := errgroup.WithContext(ctx)

	var (
		svc   *app.Service
		ready httpapi.Pinger
	)

	switch cfg.Storage {
	case "memory":
		log.Warn("STORAGE=memory: no Postgres, no Kafka; results only via POST /results")
		store := memory.New()
		svc = app.NewService(app.Deps{
			Bets: store, Results: store, Outbox: store, Tx: store,
			SettledTopic: cfg.SettledTopic, Logger: log,
		})

	case "postgres":
		db, err := postgres.Open(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		defer db.Close()
		if err := db.Migrate(ctx); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		ready = db

		svc = app.NewService(app.Deps{
			Bets: db, Results: db, Outbox: db, Tx: db,
			SettledTopic: cfg.SettledTopic, Logger: log,
		})

		producer := kafka.NewProducer(cfg.KafkaBrokers)
		defer closeLogged(log, "kafka producer", producer.Close)
		relay := outbox.NewRelay(db, db, producer, cfg.OutboxInterval, cfg.OutboxBatch, log)
		g.Go(func() error { return relay.Run(gctx) })

		consumer := kafka.NewResultConsumer(kafka.ConsumerConfig{
			Brokers:   cfg.KafkaBrokers,
			Topic:     cfg.ResultsTopic,
			GroupID:   cfg.ConsumerGroup,
			Workers:   cfg.Workers,
			BatchSize: cfg.BatchSize,
			BatchWait: cfg.BatchWait,
		}, svc, log)
		defer closeLogged(log, "kafka consumer", consumer.Close)
		g.Go(func() error { return consumer.Run(gctx) })
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewRouter(svc, ready, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	g.Go(func() error {
		log.Info("http server started", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http: %w", err)
		}
		return nil
	})

	// Shutdown goroutine: waits for a signal or a failed component, then
	// stops accepting requests and lets in-flight ones finish.
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})

	return g.Wait()
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// closeLogged is used with defer for resources whose Close error can only be logged.
func closeLogged(log *slog.Logger, name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Error("close failed", "resource", name, "err", err)
	}
}
