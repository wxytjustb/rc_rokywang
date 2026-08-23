// Command server runs the notification-delivery REST, MCP and gRPC APIs
// (DESIGN.md §4).
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
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"notification-delivery/internal/api"
	"notification-delivery/internal/application/notification"
	"notification-delivery/internal/authn"
	"notification-delivery/internal/bootstrap"
	"notification-delivery/internal/config"
	"notification-delivery/internal/grpcapi"
	"notification-delivery/internal/httpclient"
	"notification-delivery/internal/logging"
	"notification-delivery/internal/mcpapi"
	"notification-delivery/internal/mq"
	"notification-delivery/internal/publish"
	"notification-delivery/internal/store"
	"notification-delivery/internal/worker"
)

func main() {
	var configPath string

	root := &cobra.Command{
		Use:   "server",
		Short: "Notification delivery REST, MCP and gRPC server",
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
	notificationService := notification.NewService(repo, registry, publisher, logger)
	authVerifier := authn.NewVerifier(authTokens)

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
		Service:        notificationService,
		AuthVerifier:   authVerifier,
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

	var httpHandler http.Handler = router
	if cfg.MCP.Enabled {
		if err := validateMCPPath(cfg.MCP.Path); err != nil {
			return err
		}
		rootMux := http.NewServeMux()
		rootMux.Handle(cfg.MCP.Path, mcpapi.NewHandler(
			notificationService,
			authVerifier,
			logger,
			mcpapi.Options{MaxBodyBytes: cfg.MCP.MaxBodyBytes},
		))
		rootMux.Handle("/", router)
		httpHandler = rootMux
	}

	srv := &http.Server{
		Addr:         cfg.HTTP.Addr,
		Handler:      httpHandler,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
	}

	var grpcServer *grpc.Server
	var grpcHealthServer interface{ Shutdown() }
	var grpcListener net.Listener
	if cfg.GRPC.Enabled {
		if strings.TrimSpace(cfg.GRPC.Addr) == "" {
			return errors.New("grpc.addr must be configured when gRPC is enabled")
		}
		grpcListener, err = net.Listen("tcp", cfg.GRPC.Addr)
		if err != nil {
			return fmt.Errorf("listen for gRPC on %s: %w", cfg.GRPC.Addr, err)
		}
		defer grpcListener.Close()
		var grpcHealth interface{ Shutdown() }
		grpcServer, grpcHealth = grpcapi.New(
			notificationService,
			authVerifier,
			logger,
			grpcapi.Options{
				MaxReceiveMessageBytes: cfg.GRPC.MaxReceiveMessageBytes,
				ReflectionEnabled:      cfg.GRPC.ReflectionEnabled,
			},
		)
		grpcHealthServer = grpcHealth
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
		err := srv.ListenAndServe()
		if ctx.Err() == nil {
			if err == nil || errors.Is(err, http.ErrServerClosed) {
				err = errors.New("HTTP server stopped unexpectedly")
			}
			httpErrCh <- err
		}
	}()

	var grpcErrCh <-chan error
	if grpcServer != nil {
		ch := make(chan error, 1)
		grpcErrCh = ch
		go func() {
			logger.Info("grpc server listening", "addr", cfg.GRPC.Addr)
			err := grpcServer.Serve(grpcListener)
			if ctx.Err() == nil {
				if err == nil || errors.Is(err, grpc.ErrServerStopped) {
					err = errors.New("gRPC server stopped unexpectedly")
				}
				ch <- err
			}
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-httpErrCh:
		runErr = fmt.Errorf("http server: %w", err)
	case err := <-grpcErrCh:
		runErr = fmt.Errorf("grpc server: %w", err)
	case err := <-workerErrCh:
		if err != nil {
			runErr = fmt.Errorf("embedded memory worker: %w", err)
		}
	}
	// Cancel the embedded worker before closing shared dependencies. stop is
	// safe to call here and again from the deferred cleanup.
	stop()

	if grpcHealthServer != nil {
		grpcHealthServer.Shutdown()
	}
	httpShutdownTimeout := normalizedShutdownTimeout(cfg.HTTP.ShutdownTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	httpShutdownErr := srv.Shutdown(shutdownCtx)
	cancel()

	var grpcShutdownErr error
	if grpcServer != nil {
		grpcShutdownErr = gracefulStopGRPC(grpcServer, normalizedShutdownTimeout(cfg.GRPC.ShutdownTimeout))
	}
	if runErr != nil {
		return runErr
	}
	return errors.Join(httpShutdownErr, grpcShutdownErr)
}

func validateMCPPath(path string) error {
	if path == "" || path == "/" || !strings.HasPrefix(path, "/") ||
		strings.HasSuffix(path, "/") || strings.ContainsAny(path, "{} \t\r\n") {
		return fmt.Errorf("mcp.path must be a non-root exact HTTP path such as /mcp")
	}
	return nil
}

func normalizedShutdownTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 15 * time.Second
	}
	return timeout
}

func gracefulStopGRPC(server *grpc.Server, timeout time.Duration) error {
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-stopped:
		return nil
	case <-timer.C:
		server.Stop()
		<-stopped
		return fmt.Errorf("gRPC graceful shutdown exceeded %s; forced stop", timeout)
	}
}
