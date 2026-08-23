package grpcapi

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	notificationv1 "notification-delivery/gen/notification/v1"
	"notification-delivery/internal/application/notification"
	"notification-delivery/internal/authn"
	"notification-delivery/internal/bootstrap"
)

func TestListProviderCapabilitiesRequiresSharedBearerToken(t *testing.T) {
	t.Setenv("LARK_BOT_WEBHOOK_URL", "https://open.larksuite.com/open-apis/bot/v2/hook/test")
	registry, err := bootstrap.BuildRegistry("../../config/providers.yaml", nil, nil)
	if err != nil {
		t.Fatalf("BuildRegistry() error = %v", err)
	}
	server, _ := New(
		notification.NewService(nil, registry, nil, nil),
		authn.NewVerifier([]string{"shared-token"}),
		nil,
		Options{},
	)
	listener := bufconn.Listen(1024 * 1024)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := notificationv1.NewNotificationServiceClient(connection)

	healthResponse, err := grpc_health_v1.NewHealthClient(connection).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil || healthResponse.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("unauthenticated health check = (%v, %v), want SERVING", healthResponse, err)
	}
	if _, err := client.ListProviderCapabilities(context.Background(), &notificationv1.ListProviderCapabilitiesRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated status = %v, want Unauthenticated", status.Code(err))
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer shared-token")
	response, err := client.ListProviderCapabilities(ctx, &notificationv1.ListProviderCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("authenticated ListProviderCapabilities() error = %v", err)
	}
	if len(response.Providers) != 1 || response.Providers[0].ProviderCode != "lark-bot" {
		t.Fatalf("providers = %+v, want lark-bot", response.Providers)
	}
	if _, err := client.SubmitNotification(ctx, &notificationv1.SubmitNotificationRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid submit status = %v, want InvalidArgument", status.Code(err))
	}
}
