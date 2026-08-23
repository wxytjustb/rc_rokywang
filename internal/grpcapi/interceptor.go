package grpcapi

import (
	"context"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"notification-delivery/internal/authn"
)

func authUnaryInterceptor(verifier *authn.Verifier) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !isHealthMethod(info.FullMethod) {
			if err := verifyIncomingToken(ctx, verifier); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func authStreamInterceptor(verifier *authn.Verifier) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !isHealthMethod(info.FullMethod) {
			if err := verifyIncomingToken(stream.Context(), verifier); err != nil {
				return err
			}
		}
		return handler(srv, stream)
	}
}

func verifyIncomingToken(ctx context.Context, verifier *authn.Verifier) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing bearer token")
	}
	values := md.Get("authorization")
	if len(values) != 1 {
		return status.Error(codes.Unauthenticated, "missing bearer token")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || verifier.Verify(parts[1]) != nil {
		return status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	return nil
}

func isHealthMethod(method string) bool {
	return strings.HasPrefix(method, "/grpc.health.v1.Health/")
}

func loggingUnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		startedAt := time.Now()
		response, err := handler(ctx, req)
		if logger != nil {
			logger.InfoContext(ctx, "grpc request",
				"protocol", "grpc",
				"operation", info.FullMethod,
				"status", status.Code(err).String(),
				"latency_ms", time.Since(startedAt).Milliseconds())
		}
		return response, err
	}
}

func recoveryUnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if logger != nil {
					logger.ErrorContext(ctx, "grpc handler panic",
						"operation", info.FullMethod,
						"panic", recovered,
						"stack", string(debug.Stack()))
				}
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}
