package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"notification-delivery/internal/domain"
	"notification-delivery/internal/provider"
)

func TestLogProviderRequestRecordsDetailsWithoutSecrets(t *testing.T) {
	var output bytes.Buffer
	p := &Processor{Logger: slog.New(slog.NewJSONHandler(&output, nil))}
	ev := &domain.Event{
		ID:              uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
		SourceSystem:    "billing-system",
		SourceRequestID: "request-123",
		ProviderCode:    "lark-bot",
		ProviderAction:  "send",
		Payload:         json.RawMessage(`{"text":"confidential message"}`),
		AttemptCount:    2,
	}
	ac := provider.ActionContext{
		Method:     "POST",
		Path:       "https://open.example.com/open-apis/bot/v2/hook/webhook-secret?token=query-secret",
		TimeoutMs:  5000,
		Credential: "credential-secret",
	}
	result := &provider.Result{
		HTTPStatus:       503,
		ErrorClass:       "VENDOR_TRANSIENT_ERROR",
		Message:          "response-secret",
		ProviderResponse: json.RawMessage(`{"body":"response-secret"}`),
	}

	p.logProviderRequest(context.Background(), ev, ac, result, 125*time.Millisecond)

	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode provider request log: %v", err)
	}
	want := map[string]any{
		"level":             "INFO",
		"msg":               "provider request completed",
		"event_id":          ev.ID.String(),
		"source_system":     "billing-system",
		"source_request_id": "request-123",
		"provider_code":     "lark-bot",
		"provider_action":   "send",
		"status":            "RETRYABLE_FAILURE",
		"failure_reason":    "VENDOR_TRANSIENT_ERROR",
	}
	for field, expected := range want {
		if entry[field] != expected {
			t.Errorf("%s = %v, want %v", field, entry[field], expected)
		}
	}
	for field, expected := range map[string]float64{
		"attempt_number":          2,
		"timeout_ms":              5000,
		"payload_bytes":           float64(len(ev.Payload)),
		"latency_ms":              125,
		"provider_response_bytes": float64(len(result.ProviderResponse)),
	} {
		if entry[field] != expected {
			t.Errorf("%s = %v, want %v", field, entry[field], expected)
		}
	}
	for _, field := range []string{"http_method", "http_status", "target_scheme", "target_host"} {
		if _, ok := entry[field]; ok {
			t.Errorf("provider request log contains protocol-specific field %q", field)
		}
	}
	for _, secret := range []string{"webhook-secret", "query-secret", "credential-secret", "confidential message", "response-secret"} {
		if strings.Contains(output.String(), secret) {
			t.Errorf("provider request log contains secret %q", secret)
		}
	}
}

func TestLogProviderRequestSuccessOmitsFailureReason(t *testing.T) {
	var output bytes.Buffer
	p := &Processor{Logger: slog.New(slog.NewJSONHandler(&output, nil))}
	p.logProviderRequest(context.Background(), &domain.Event{ID: uuid.New()}, provider.ActionContext{}, &provider.Result{
		Success: true,
	}, time.Millisecond)

	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode provider request log: %v", err)
	}
	if entry["status"] != "SUCCESS" {
		t.Errorf("status = %v, want SUCCESS", entry["status"])
	}
	if _, ok := entry["failure_reason"]; ok {
		t.Error("successful provider request log contains failure_reason")
	}
}
