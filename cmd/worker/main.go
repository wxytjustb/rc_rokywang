// Command worker runs the Delivery Worker plus its embedded compensator
// (DESIGN.md §2.2, §8, §9). Multiple instances against the same MQ
// topic/channel or queue and the same PostgreSQL database are competing
// consumers with no shared in-process state — that is the entire
// horizontal-scaling mechanism.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"notification-delivery/internal/bootstrap"
	"notification-delivery/internal/config"
	"notification-delivery/internal/httpclient"
	"notification-delivery/internal/logging"
	"notification-delivery/internal/mq"
	"notification-delivery/internal/publish"
	"notification-delivery/internal/store"
	"notification-delivery/internal/worker"
)

func main() {
	var configPath string

	root := &cobra.Command{
		Use:   "worker",
		Short: "Notification delivery worker",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), configPath)
		},
	}
	root.Flags().StringVar(&configPath, "config", "config/worker.yaml", "path to worker config YAML")

	if err := root.ExecuteContext(context.Background()); err != nil {
		slog.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run(parentCtx context.Context, configPath string) error {
	logger := logging.NewJSON(os.Stdout)

	cfg, err := config.LoadWorker(configPath)
	if err != nil {
		return err
	}
	if cfg.MQ.Driver == "memory" {
		return fmt.Errorf("mq driver \"memory\" is in-process only; run cmd/server with its embedded worker")
	}
	ctx, stop := signal.NotifyContext(parentCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Database.AutoCreate {
		if err := store.EnsureDatabase(ctx, cfg.Database.DSN, cfg.Database.Driver); err != nil {
			return fmt.Errorf("ensure database: %w", err)
		}
	}

	pool, err := store.NewPool(ctx, cfg.Database.DSN, cfg.Database.MaxConns, cfg.Database.MinConns, cfg.Database.Driver)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	sqlDB, err := pool.DB()
	if err != nil {
		return fmt.Errorf("resolve database handle: %w", err)
	}
	defer func() {
		_ = sqlDB.Close()
	}()

	if cfg.AutoMigrate {
		if err := store.Migrate(ctx, pool); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	repo := store.NewEventRepo(pool)

	httpClient := httpclient.New(cfg.HTTPClient.MaxResponseBytes)
	registry, err := bootstrap.BuildRegistry(cfg.ProvidersFile, httpClient, cfg.HTTPClient.AllowedRespHeaders)
	if err != nil {
		return err
	}

	limiters := worker.NewLimiters()
	breakers := worker.NewBreakers(logger)
	for key, ra := range registry.All() {
		limiters.Register(key, ra.RequestsPerSecond, ra.MaxConcurrency)
		breakers.Register(key, ra.CircuitBreaker)
	}

	broker, err := mq.NewPublisher(cfg.MQ)
	if err != nil {
		return fmt.Errorf("connect to mq publisher: %w", err)
	}
	defer broker.Close()
	publisher := publish.New(broker, repo)

	consumer, err := mq.NewConsumer(cfg.MQ)
	if err != nil {
		return fmt.Errorf("connect to mq consumer: %w", err)
	}

	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	processor := &worker.Processor{
		Repo:          repo,
		Registry:      registry,
		Limiters:      limiters,
		Breakers:      breakers,
		LeaseDuration: cfg.Lease.Duration,
		Logger:        logger,
	}

	w := &worker.Worker{
		Consumer:  consumer,
		Processor: processor,
		Compensator: &worker.Compensator{
			Repo:          repo,
			Publisher:     publisher,
			Logger:        logger,
			Cfg:           cfg.Compensator,
			LeaseDuration: cfg.Lease.Duration,
		},
		Concurrency: concurrency,
		Logger:      logger,
	}

	logger.Info("worker starting", "mq_driver", cfg.MQ.Driver, "concurrency", concurrency)
	return w.Run(ctx)
}
