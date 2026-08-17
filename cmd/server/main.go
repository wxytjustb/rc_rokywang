// Command server runs the notification-delivery HTTP API (DESIGN.md §4).
// With an external MQ it is stateless aside from PostgreSQL and the broker.
// With mq.driver=memory it also embeds the delivery worker in this process.
//
// @title Notification Delivery API
// @version 1.0
// @description Reliably accepts notifications for asynchronous delivery and exposes their delivery status.
// @BasePath /
// @schemes http https
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
// @description Swagger UI accepts the raw token and adds the Bearer prefix automatically. Direct HTTP clients must send Authorization: Bearer {token}.
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

	"github.com/spf13/cobra"

	"notification-delivery/internal/api"
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
		Use:   "server",
		Short: "Notification delivery HTTP API server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), configPath)
		},
	}
	root.Flags().StringVar(&configPath, "config", "config/server.yaml", "path to server config YAML")

	if err := root.ExecuteContext(context.Background()); err != nil {
		slog.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

func run(parentCtx context.Context, configPath string) error {
	logger := logging.NewJSON(os.Stdout)

	cfg, err := config.LoadServer(configPath)
	if err != nil {
		return err
	}
	authTokens, generatedToken, err := config.ResolveAuthTokens(cfg.Auth.Tokens)
	if err != nil {
		return err
	}
	if generatedToken != "" {
		logger.Info("no auth tokens configured; generated bearer token", "token", generatedToken)
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

	var deliveryClient *httpclient.Client
	var allowedRespHeaders []string
	if cfg.MQ.Driver == "memory" {
		if cfg.Worker.Lease.Duration <= 0 {
			return fmt.Errorf("worker.lease.duration must be greater than zero for mq driver \"memory\"")
		}
		deliveryClient = httpclient.New(cfg.Worker.HTTPClient.MaxResponseBytes)
		allowedRespHeaders = cfg.Worker.HTTPClient.AllowedRespHeaders
	}

	// External-MQ server instances only validate requests. Memory mode also
	// sends vendor requests, so the same registry receives a delivery client.
	registry, err := bootstrap.BuildRegistry(cfg.ProvidersFile, deliveryClient, allowedRespHeaders)
	if err != nil {
		return err
	}

	var broker mq.Publisher
	var memoryBroker *mq.MemoryBroker
	if cfg.MQ.Driver == "memory" {
		memoryBroker, err = mq.NewMemoryBroker(cfg.MQ.Memory, logger)
		if err != nil {
			return fmt.Errorf("create memory mq: %w", err)
		}
		broker = memoryBroker
	} else {
		broker, err = mq.NewPublisher(cfg.MQ)
		if err != nil {
			return fmt.Errorf("connect to mq: %w", err)
		}
	}
	defer broker.Close()

	publisher := publish.New(broker, repo)

	var embeddedWorker *worker.Worker
	if memoryBroker != nil {
		limiters := worker.NewLimiters()
		breakers := worker.NewBreakers(logger)
		for key, ra := range registry.All() {
			limiters.Register(key, ra.RequestsPerSecond, ra.MaxConcurrency)
			breakers.Register(key, ra.CircuitBreaker)
		}
		concurrency := cfg.Worker.Concurrency
		if concurrency <= 0 {
			concurrency = 1
		}
		embeddedWorker = &worker.Worker{
			Consumer: memoryBroker,
			Processor: &worker.Processor{
				Repo:          repo,
				Registry:      registry,
				Limiters:      limiters,
				Breakers:      breakers,
				LeaseDuration: cfg.Worker.Lease.Duration,
				Logger:        logger,
			},
			Compensator: &worker.Compensator{
				Repo:          repo,
				Publisher:     publisher,
				Logger:        logger,
				Cfg:           cfg.Worker.Compensator,
				LeaseDuration: cfg.Worker.Lease.Duration,
			},
			Concurrency: concurrency,
			Logger:      logger,
		}
	}

	router := api.NewRouter(api.Deps{
		Repo:           repo,
		Registry:       registry,
		Publisher:      publisher,
		Logger:         logger,
		AuthTokens:     authTokens,
		MaxBodyBytes:   cfg.HTTP.MaxBodyBytes,
		SwaggerEnabled: cfg.Swagger.Enabled,
		Ready: func() error {
			pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := sqlDB.PingContext(pingCtx); err != nil {
				return err
			}
			if memoryBroker != nil {
				return memoryBroker.Ready()
			}
			return nil
		},
	})

	srv := &http.Server{
		Addr:         cfg.HTTP.Addr,
		Handler:      router,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
	}

	workerErrCh := make(chan error, 1)
	if embeddedWorker != nil {
		logger.Info("embedded memory worker starting",
			"concurrency", embeddedWorker.Concurrency,
			"buffer_size", cfg.MQ.Memory.BufferSize)
		go func() {
			err := embeddedWorker.Run(ctx)
			if err == nil && ctx.Err() == nil {
				err = errors.New("embedded memory worker stopped unexpectedly")
			}
			workerErrCh <- err
		}()
	}

	httpErrCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.HTTP.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErrCh <- err
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-httpErrCh:
		runErr = fmt.Errorf("http server: %w", err)
	case err := <-workerErrCh:
		if err != nil {
			runErr = fmt.Errorf("embedded memory worker: %w", err)
		}
	}
	// Cancel the embedded worker before closing shared dependencies. stop is
	// safe to call here and again from the deferred cleanup.
	stop()

	shutdownTimeout := cfg.HTTP.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = 15 * time.Second
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	if runErr != nil {
		return runErr
	}
	return shutdownErr
}
