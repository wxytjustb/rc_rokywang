package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestAccessLoggerLogsEveryRequestAtInfo(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	router := NewRouter(Deps{Logger: logger})

	tests := []struct {
		method string
		path   string
		status int
	}{
		{method: http.MethodGet, path: "/healthz", status: http.StatusOK},
		{method: http.MethodGet, path: "/missing", status: http.StatusNotFound},
		{method: http.MethodPost, path: "/healthz", status: http.StatusMethodNotAllowed},
	}

	for _, tt := range tests {
		request := httptest.NewRequest(tt.method, tt.path, nil)
		request.Header.Set("Authorization", "Bearer must-not-be-logged")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != tt.status {
			t.Fatalf("%s %s status = %d, want %d", tt.method, tt.path, response.Code, tt.status)
		}
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != len(tests) {
		t.Fatalf("access log lines = %d, want %d: %s", len(lines), len(tests), output.String())
	}
	if strings.Contains(output.String(), "must-not-be-logged") {
		t.Fatal("access log contains the Authorization token")
	}

	for i, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("decode access log line %d: %v", i, err)
		}
		if entry["level"] != "INFO" || entry["msg"] != "http request" {
			t.Errorf("access log line %d level/message = %v/%v", i, entry["level"], entry["msg"])
		}
		if entry["method"] != tests[i].method || entry["path"] != tests[i].path {
			t.Errorf("access log line %d request = %v %v, want %s %s", i, entry["method"], entry["path"], tests[i].method, tests[i].path)
		}
		if entry["status"] != float64(tests[i].status) {
			t.Errorf("access log line %d status = %v, want %d", i, entry["status"], tests[i].status)
		}
		for _, field := range []string{"latency_ms", "client_ip"} {
			if _, ok := entry[field]; !ok {
				t.Errorf("access log line %d is missing %s", i, field)
			}
		}
	}
}
