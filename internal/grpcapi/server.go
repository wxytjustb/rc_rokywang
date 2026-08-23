package grpcapi

import (
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	notificationv1 "notification-delivery/gen/notification/v1"
	"notification-delivery/internal/application/notification"
	"notification-delivery/internal/authn"
)

type Options struct {
	MaxReceiveMessageBytes int
	ReflectionEnabled      bool
}

func New(service *notification.Service, verifier *authn.Verifier, logger *slog.Logger, opts Options) (*grpc.Server, *health.Server) {
	serverOptions := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			recoveryUnaryInterceptor(logger),
			authUnaryInterceptor(verifier),
			loggingUnaryInterceptor(logger),
		),
		grpc.ChainStreamInterceptor(
			authStreamInterceptor(verifier),
		),
	}
	if opts.MaxReceiveMessageBytes > 0 {
		serverOptions = append(serverOptions, grpc.MaxRecvMsgSize(opts.MaxReceiveMessageBytes))
	}

	grpcServer := grpc.NewServer(serverOptions...)
	notificationv1.RegisterNotificationServiceServer(grpcServer, &notificationServer{service: service})

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	if opts.ReflectionEnabled {
		reflection.Register(grpcServer)
	}
	return grpcServer, healthServer
}
