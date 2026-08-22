package adapters

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"notification-delivery/internal/httpclient"
	"notification-delivery/internal/provider"
)

func TestLoadtestHTTPConfigHasNoRateCap(t *testing.T) {
	cfg, err := LoadtestHTTP{}.Config(configNode(t, `
actions:
  request:
    endpoint_url: http://127.0.0.1:18080/loadtest
    timeout_ms: 5000
    max_concurrency: 256
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	action := cfg.Actions[loadtestActionRequest]
	if action.Method != http.MethodPost || action.RequestsPerSecond != 0 || action.MaxConcurrency != 256 {
		t.Fatalf("normalized action = %+v", action)
	}
}

func TestLoadtestHTTPConfigRejectsNonLoopbackEndpoint(t *testing.T) {
	_, err := LoadtestHTTP{}.Config(configNode(t, `
actions:
  request:
    endpoint_url: http://example.com/loadtest
    timeout_ms: 5000
    max_concurrency: 10
`))
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Config() error = %v, want loopback validation error", err)
	}
}

func TestLoadtestHTTPSendsPayloadAndUsesHTTPStatus(t *testing.T) {
	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		received, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	payload := json.RawMessage(`{"sequence":1}`)
	result, err := LoadtestHTTP{}.SendActionRequest(context.Background(), provider.ActionContext{
		Path:       server.URL + "/loadtest",
		Method:     http.MethodPost,
		TimeoutMs:  1000,
		HTTPClient: httpclient.New(1024),
	}, loadtestActionRequest, payload)
	if err != nil {
		t.Fatalf("SendActionRequest() error = %v", err)
	}
	if !result.Success || result.HTTPStatus != http.StatusNoContent {
		t.Fatalf("result = %+v", result)
	}
	if string(received) != string(payload) {
		t.Fatalf("received body = %s, want %s", received, payload)
	}
}

func TestLoadtestHTTPMarksUnavailableStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	result, err := LoadtestHTTP{}.SendActionRequest(context.Background(), provider.ActionContext{
		Path:       server.URL,
		Method:     http.MethodPost,
		TimeoutMs:  1000,
		HTTPClient: httpclient.New(1024),
	}, loadtestActionRequest, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("SendActionRequest() error = %v", err)
	}
	if result.Success || !result.AvailabilityFailure || result.ErrorClass != "LOADTEST_HTTP_STATUS" {
		t.Fatalf("result = %+v", result)
	}
}
