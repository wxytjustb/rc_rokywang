package adapters

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"notification-delivery/internal/httpclient"
	"notification-delivery/internal/provider"
)

func TestWebhookConfigNormalizesFixedEndpointAndAuthentication(t *testing.T) {
	adapter := NewWebhook()
	cfg, err := adapter.Config(configNode(t, `
credential_ref: vault://notification/webhook-secret
actions:
  deliver:
    endpoint_url: https://events.example.com/notifications
    authentication: hmac_sha256
    timeout_ms: 5000
    requests_per_second: 20
    max_concurrency: 10
    circuit_breaker:
      failure_threshold: 5
      open_duration: 30s
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	action := cfg.Actions[webhookActionDeliver]
	if cfg.CredentialRef != "vault://notification/webhook-secret" || adapter.authentication != webhookAuthHMAC {
		t.Fatalf("config = %+v authentication = %q", cfg, adapter.authentication)
	}
	if action.Method != http.MethodPost || action.Path != "https://events.example.com/notifications" {
		t.Fatalf("action = %+v", action)
	}
}

func TestWebhookConfigRejectsUnsafeDestinationOrAuthMismatch(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "remote HTTP",
			body: `actions:
  deliver:
    endpoint_url: http://events.example.com/notifications
    authentication: none
    timeout_ms: 1000
    requests_per_second: 1
    max_concurrency: 1`,
			want: "must use HTTPS",
		},
		{
			name: "query credential",
			body: `actions:
  deliver:
    endpoint_url: https://events.example.com/notifications?token=secret
    authentication: none
    timeout_ms: 1000
    requests_per_second: 1
    max_concurrency: 1`,
			want: "query parameters",
		},
		{
			name: "bearer without credential",
			body: `actions:
  deliver:
    endpoint_url: https://events.example.com/notifications
    authentication: bearer
    timeout_ms: 1000
    requests_per_second: 1
    max_concurrency: 1`,
			want: "credential_ref is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewWebhook().Config(configNode(t, tt.body))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Config() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestWebhookValidateRequiresJSONObject(t *testing.T) {
	adapter := NewWebhook()
	if err := adapter.Validate(webhookActionDeliver, json.RawMessage(`{"event":"updated"}`)); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	for _, payload := range []string{`[]`, `null`, `"text"`, `{`} {
		if err := adapter.Validate(webhookActionDeliver, json.RawMessage(payload)); err == nil {
			t.Errorf("Validate(%s) error = nil", payload)
		}
	}
}

func TestWebhookIdempotencyKeyIncludesSourceSystem(t *testing.T) {
	first := webhookIdempotencyKey(webhookProviderCode, "source-a", "request-1")
	second := webhookIdempotencyKey(webhookProviderCode, "source-b", "request-1")
	if first == second {
		t.Fatalf("different source systems produced the same idempotency key %q", first)
	}
}

func TestWebhookDeliverSendsHMACAndStableIdempotencyKey(t *testing.T) {
	payload := json.RawMessage(`{"event":"updated","id":123}`)
	secret := "test-secret"
	var requestErr atomic.Value
	server := newTCP4HTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != string(payload) {
			requestErr.Store("unexpected body: " + string(body))
		}
		timestamp := r.Header.Get("X-Webhook-Timestamp")
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write([]byte(timestamp + "."))
		_, _ = mac.Write(payload)
		wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if r.Header.Get("X-Webhook-Signature") != wantSignature {
			requestErr.Store("unexpected signature")
		}
		wantKey := webhookIdempotencyKey(webhookProviderCode, "source-a", "request-1")
		if r.Header.Get("Idempotency-Key") != wantKey {
			requestErr.Store("unexpected idempotency key")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	}))

	adapter := NewWebhook()
	_, err := adapter.Config(configNode(t, `
credential_ref: vault://notification/webhook-secret
actions:
  deliver:
    endpoint_url: `+server.URL+`
    authentication: hmac_sha256
    timeout_ms: 3000
    requests_per_second: 10
    max_concurrency: 2
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	result, err := adapter.SendActionRequest(context.Background(), provider.ActionContext{
		ProviderCode:       webhookProviderCode,
		ProviderAction:     webhookActionDeliver,
		Path:               server.URL,
		Method:             http.MethodPost,
		Credential:         secret,
		SourceSystem:       "source-a",
		SourceRequestID:    "request-1",
		TimeoutMs:          3000,
		HTTPClient:         httpclient.New(4096),
		AllowedRespHeaders: []string{"Content-Type"},
	}, webhookActionDeliver, payload)
	if err != nil {
		t.Fatalf("SendActionRequest() error = %v", err)
	}
	if result == nil || !result.Success || result.HTTPStatus != http.StatusAccepted {
		t.Fatalf("result = %+v", result)
	}
	if value := requestErr.Load(); value != nil {
		t.Fatal(value)
	}
}

func TestWebhookRejectsRedirectWithoutCallingTarget(t *testing.T) {
	var targetCalls atomic.Int32
	target := newTCP4HTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	redirect := newTCP4HTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))

	adapter := NewWebhook()
	_, err := adapter.Config(configNode(t, `
actions:
  deliver:
    endpoint_url: `+redirect.URL+`
    authentication: none
    timeout_ms: 3000
    requests_per_second: 10
    max_concurrency: 2
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	result, err := adapter.SendActionRequest(context.Background(), provider.ActionContext{
		ProviderCode:    webhookProviderCode,
		ProviderAction:  webhookActionDeliver,
		Path:            redirect.URL,
		Method:          http.MethodPost,
		SourceSystem:    "source-a",
		SourceRequestID: "request-1",
		TimeoutMs:       3000,
		HTTPClient:      httpclient.New(4096),
	}, webhookActionDeliver, json.RawMessage(`{"event":"test"}`))
	if err != nil {
		t.Fatalf("SendActionRequest() error = %v", err)
	}
	if result == nil || result.Success || result.ErrorClass != "WEBHOOK_REDIRECT_REJECTED" || result.HTTPStatus != http.StatusTemporaryRedirect {
		t.Fatalf("result = %+v", result)
	}
	if got := targetCalls.Load(); got != 0 {
		t.Fatalf("redirect target calls = %d, want 0", got)
	}
}

func TestInterpretWebhookResponse(t *testing.T) {
	tests := []struct {
		status              int
		success             bool
		availabilityFailure bool
		errorClass          string
	}{
		{status: 204, success: true},
		{status: 302, errorClass: "WEBHOOK_REDIRECT_REJECTED"},
		{status: 400, errorClass: "WEBHOOK_HTTP_ERROR"},
		{status: 408, availabilityFailure: true, errorClass: "WEBHOOK_HTTP_ERROR"},
		{status: 429, availabilityFailure: true, errorClass: "WEBHOOK_HTTP_ERROR"},
		{status: 503, availabilityFailure: true, errorClass: "WEBHOOK_HTTP_ERROR"},
	}
	for _, tt := range tests {
		got := interpretWebhookResponse(&httpclient.Response{StatusCode: tt.status})
		if got.Success != tt.success || got.AvailabilityFailure != tt.availabilityFailure || got.ErrorClass != tt.errorClass {
			t.Errorf("status %d result = %+v", tt.status, got)
		}
	}
}

func newTCP4HTTPServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for HTTP test server: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server
}
