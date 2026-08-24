package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"

	"notification-delivery/internal/provider"
)

type fakeFirebaseMessagingClient struct {
	messageID string
	err       error
	messages  []*messaging.Message
}

func (f *fakeFirebaseMessagingClient) Send(_ context.Context, message *messaging.Message) (string, error) {
	f.messages = append(f.messages, message)
	return f.messageID, f.err
}

func TestFirebasePushConfigNormalizesAction(t *testing.T) {
	adapter := NewFirebasePush()
	cfg, err := adapter.Config(configNode(t, `
project_id: mobile-app-123
credentials_ref: vault://notification/firebase-service-account-json
actions:
  send:
    timeout_ms: 10000
    requests_per_second: 100
    max_concurrency: 20
    circuit_breaker:
      failure_threshold: 5
      open_duration: 30s
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	action := cfg.Actions[firebasePushActionSend]
	if cfg.CredentialRef != "vault://notification/firebase-service-account-json" {
		t.Fatalf("credential ref = %q", cfg.CredentialRef)
	}
	if action.Method != "FCM" || action.TimeoutMs != 10000 || action.MaxConcurrency != 20 {
		t.Fatalf("normalized action = %+v", action)
	}
	if action.CircuitBreaker == nil || action.CircuitBreaker.OpenDuration != 30*time.Second {
		t.Fatalf("normalized circuit breaker = %+v", action.CircuitBreaker)
	}
	if !adapter.initialized || adapter.projectID != "mobile-app-123" || !adapter.credentialsRequired {
		t.Fatalf("adapter runtime config = %+v", adapter)
	}
}

func TestFirebasePushConfigAllowsApplicationDefaultCredentials(t *testing.T) {
	adapter := NewFirebasePush()
	cfg, err := adapter.Config(configNode(t, `
project_id: mobile-app-123
actions:
  send:
    timeout_ms: 5000
    requests_per_second: 10
    max_concurrency: 4
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	if cfg.CredentialRef != "" || adapter.credentialsRequired {
		t.Fatalf("ADC config = credential ref %q, required %v", cfg.CredentialRef, adapter.credentialsRequired)
	}
}

func TestFirebasePushConfigRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "missing project",
			body: `actions:
  send:
    timeout_ms: 5000
    requests_per_second: 10
    max_concurrency: 4`,
			want: "project_id",
		},
		{
			name: "invalid limits",
			body: `project_id: mobile-app-123
actions:
  send:
    timeout_ms: 0
    requests_per_second: 10
    max_concurrency: 4`,
			want: "timeout_ms",
		},
		{
			name: "unknown field",
			body: `project_id: mobile-app-123
credential_ref: vault://wrong-field
actions:
  send:
    timeout_ms: 5000
    requests_per_second: 10
    max_concurrency: 4`,
			want: "field credential_ref not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFirebasePush().Config(configNode(t, tt.body))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Config() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestFirebasePushValidate(t *testing.T) {
	adapter := NewFirebasePush()
	valid := []string{
		`{"fid":"firebase-installation-id","notification":{"title":"Order ready","body":"Order 123 is ready"}}`,
		`{"token":"legacy-registration-token","data":{"order_id":"123"},"android":{"priority":"high"},"ios":{"content_available":true}}`,
	}
	for _, payload := range valid {
		if err := adapter.Validate(firebasePushActionSend, json.RawMessage(payload)); err != nil {
			t.Errorf("Validate(%s) error = %v", payload, err)
		}
	}

	invalid := []struct {
		name    string
		payload string
		want    string
	}{
		{"missing target", `{"data":{"order_id":"123"}}`, "exactly one"},
		{"multiple targets", `{"fid":"fid","token":"token","data":{"order_id":"123"}}`, "exactly one"},
		{"missing content", `{"fid":"fid"}`, "notification or payload.data"},
		{"empty notification", `{"fid":"fid","notification":{}}`, "title or payload.notification.body"},
		{"reserved data", `{"fid":"fid","data":{"google.test":"value"}}`, "reserved"},
		{"bad priority", `{"fid":"fid","data":{"id":"1"},"android":{"priority":"urgent"}}`, "priority"},
		{"negative badge", `{"fid":"fid","data":{"id":"1"},"ios":{"badge":-1}}`, "badge"},
		{"insecure image", `{"fid":"fid","notification":{"title":"Title","image_url":"http://example.com/image.png"}}`, "HTTPS"},
		{"unknown field", `{"fid":"fid","data":{"id":"1"},"topic":"all"}`, "unknown field"},
		{"trailing JSON", `{"fid":"fid","data":{"id":"1"}} {}`, "exactly one JSON value"},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			err := adapter.Validate(firebasePushActionSend, json.RawMessage(tt.payload))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestBuildFirebaseMessageMapsAndroidAndIOSOverrides(t *testing.T) {
	message, err := buildFirebaseMessage(json.RawMessage(`{
  "fid":"firebase-installation-id",
  "notification":{"title":"Order ready","body":"Order 123 is ready","image_url":"https://cdn.example.com/order.png"},
  "data":{"order_id":"123"},
  "android":{"priority":"high","channel_id":"orders","sound":"default"},
  "ios":{"sound":"default","badge":7,"content_available":true}
}`))
	if err != nil {
		t.Fatalf("buildFirebaseMessage() error = %v", err)
	}
	if message.Fid != "firebase-installation-id" || message.Token != "" || message.Data["order_id"] != "123" {
		t.Fatalf("message target/data = %+v", message)
	}
	if message.Android == nil || message.Android.Priority != "high" || message.Android.Notification == nil || message.Android.Notification.ChannelID != "orders" {
		t.Fatalf("Android config = %+v", message.Android)
	}
	if message.APNS == nil || message.APNS.Payload == nil || message.APNS.Payload.Aps == nil {
		t.Fatalf("APNS config = %+v", message.APNS)
	}
	aps := message.APNS.Payload.Aps
	if aps.Sound != "default" || aps.Badge == nil || *aps.Badge != 7 || !aps.ContentAvailable || !aps.MutableContent {
		t.Fatalf("APNS aps = %+v", aps)
	}
	if message.APNS.FCMOptions == nil || message.APNS.FCMOptions.ImageURL != "https://cdn.example.com/order.png" {
		t.Fatalf("APNS FCM options = %+v", message.APNS.FCMOptions)
	}
}

func TestFirebasePushSendCachesClientAndRecordsAcceptance(t *testing.T) {
	fakeClient := &fakeFirebaseMessagingClient{messageID: "projects/mobile-app-123/messages/message-1"}
	factoryCalls := 0
	adapter := NewFirebasePush()
	adapter.newClient = func(_ context.Context, projectID, credential string) (firebaseMessagingClient, error) {
		factoryCalls++
		if projectID != "mobile-app-123" || credential != `{"type":"service_account"}` {
			t.Fatalf("factory inputs = project %q credential %q", projectID, credential)
		}
		return fakeClient, nil
	}
	configureFirebasePushForTest(t, adapter, true)

	payload := json.RawMessage(`{"fid":"firebase-installation-id","notification":{"title":"Title"}}`)
	for range 2 {
		result, err := adapter.SendActionRequest(context.Background(), provider.ActionContext{
			Credential: `{"type":"service_account"}`,
			TimeoutMs:  1000,
		}, firebasePushActionSend, payload)
		if err != nil {
			t.Fatalf("SendActionRequest() error = %v", err)
		}
		if !result.Success || result.AvailabilityFailure || !strings.Contains(string(result.ProviderResponse), "message-1") {
			t.Fatalf("SendActionRequest() result = %+v", result)
		}
		if strings.Contains(string(result.ProviderResponse), "firebase-installation-id") {
			t.Fatal("provider response contains the target FID")
		}
	}
	if factoryCalls != 1 || len(fakeClient.messages) != 2 {
		t.Fatalf("factory calls = %d, sends = %d", factoryCalls, len(fakeClient.messages))
	}
}

func TestFirebasePushSendClassifiesFailures(t *testing.T) {
	tests := []struct {
		name         string
		factoryError error
		sendError    error
		messageID    string
		wantClass    string
		availability bool
	}{
		{name: "client init", factoryError: errors.New("invalid credential"), wantClass: "FCM_CLIENT_INIT_ERROR"},
		{name: "timeout", sendError: context.DeadlineExceeded, wantClass: "FCM_TIMEOUT", availability: true},
		{name: "generic send", sendError: errors.New("send failed"), wantClass: "FCM_SEND_ERROR"},
		{name: "missing message id", wantClass: "FCM_PROTOCOL_ERROR", availability: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := NewFirebasePush()
			adapter.newClient = func(context.Context, string, string) (firebaseMessagingClient, error) {
				if tt.factoryError != nil {
					return nil, tt.factoryError
				}
				return &fakeFirebaseMessagingClient{messageID: tt.messageID, err: tt.sendError}, nil
			}
			configureFirebasePushForTest(t, adapter, false)
			result, err := adapter.SendActionRequest(context.Background(), provider.ActionContext{TimeoutMs: 1000}, firebasePushActionSend, json.RawMessage(`{"fid":"fid","data":{"id":"1"}}`))
			if err != nil {
				t.Fatalf("SendActionRequest() error = %v", err)
			}
			if result.ErrorClass != tt.wantClass || result.AvailabilityFailure != tt.availability {
				t.Fatalf("result = %+v, want class %q availability %v", result, tt.wantClass, tt.availability)
			}
		})
	}
}

func TestFirebasePushSendRejectsEmptyResolvedCredential(t *testing.T) {
	adapter := NewFirebasePush()
	configureFirebasePushForTest(t, adapter, true)
	result, err := adapter.SendActionRequest(context.Background(), provider.ActionContext{TimeoutMs: 1000}, firebasePushActionSend, json.RawMessage(`{"fid":"fid","data":{"id":"1"}}`))
	if err != nil {
		t.Fatalf("SendActionRequest() error = %v", err)
	}
	if result.ErrorClass != "FCM_CREDENTIAL_EMPTY" {
		t.Fatalf("result = %+v", result)
	}
}

func TestNewFirebaseMessagingClientRejectsNonServiceAccountCredential(t *testing.T) {
	_, err := newFirebaseMessagingClient(context.Background(), "mobile-app-123", `{"type":"external_account"}`)
	if err == nil || !strings.Contains(err.Error(), "service_account") {
		t.Fatalf("newFirebaseMessagingClient() error = %v, want service-account validation error", err)
	}
}

func TestClassifyFirebasePushFailureRecognizesUnregisteredTarget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"status":"INVALID_ARGUMENT","message":"target is not registered","details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}`))
	}))
	defer server.Close()

	app, err := firebase.NewApp(context.Background(), &firebase.Config{ProjectID: "mobile-app-123"}, option.WithEndpoint(server.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("firebase.NewApp() error = %v", err)
	}
	client, err := app.Messaging(context.Background())
	if err != nil {
		t.Fatalf("app.Messaging() error = %v", err)
	}
	_, sendErr := client.Send(context.Background(), &messaging.Message{Fid: "fid", Data: map[string]string{"id": "1"}})
	if sendErr == nil {
		t.Fatal("client.Send() error = nil")
	}
	result := classifyFirebasePushFailure(context.Background(), sendErr, "send")
	if result.ErrorClass != "FCM_TARGET_UNREGISTERED" || result.AvailabilityFailure || result.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("result = %+v", result)
	}
}

func configureFirebasePushForTest(t *testing.T, adapter *FirebasePush, withCredential bool) {
	t.Helper()
	credentialLine := ""
	if withCredential {
		credentialLine = "credentials_ref: vault://notification/firebase-service-account-json\n"
	}
	_, err := adapter.Config(configNode(t, "project_id: mobile-app-123\n"+credentialLine+`actions:
  send:
    timeout_ms: 1000
    requests_per_second: 10
    max_concurrency: 4
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
}
