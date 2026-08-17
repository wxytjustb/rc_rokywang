package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestJSONEndpointsUseUnifiedEnvelope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(Deps{
		AuthTokens: []string{"dev-system-token"},
		Ready:      func() error { return nil },
	})

	tests := []struct {
		name       string
		path       string
		method     string
		headers    map[string]string
		httpStatus int
		status     int
		message    string
	}{
		{name: "health", path: "/healthz", method: http.MethodGet, httpStatus: http.StatusOK, status: 0},
		{name: "ready", path: "/readyz", method: http.MethodGet, httpStatus: http.StatusOK, status: 0},
		{name: "unauthenticated english", path: "/v1/messages/id", method: http.MethodGet, httpStatus: http.StatusUnauthorized, status: 1002, message: "Authentication failed. Check the bearer token."},
		{name: "unauthenticated chinese", path: "/v1/messages/id", method: http.MethodGet, headers: map[string]string{"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"}, httpStatus: http.StatusUnauthorized, status: 1002, message: "身份认证失败，请检查 Bearer Token。"},
		{name: "not found", path: "/missing", method: http.MethodGet, httpStatus: http.StatusNotFound, status: 1008, message: "The requested API route does not exist."},
		{name: "trailing slash does not bypass envelope", path: "/healthz/", method: http.MethodGet, httpStatus: http.StatusNotFound, status: 1008, message: "The requested API route does not exist."},
		{name: "method not allowed", path: "/healthz", method: http.MethodPost, httpStatus: http.StatusMethodNotAllowed, status: 1009, message: "The HTTP method is not allowed for this API route."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(tt.method, tt.path, nil)
			for name, value := range tt.headers {
				request.Header.Set(name, value)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != tt.httpStatus {
				t.Fatalf("HTTP status = %d, want %d", response.Code, tt.httpStatus)
			}
			var envelope struct {
				Status       int             `json:"status"`
				Data         json.RawMessage `json:"data"`
				ErrorMessage string          `json:"error_message"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if envelope.Status != tt.status || envelope.ErrorMessage != tt.message {
				t.Fatalf("response = %+v, want status=%d error_message=%q", envelope, tt.status, tt.message)
			}
			if len(envelope.Data) == 0 {
				t.Fatal("response is missing data")
			}
		})
	}
}

func TestReadinessErrorIsLocalized(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(Deps{Ready: func() error { return errors.New("database password must not leak") }})
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	request.Header.Set("Accept-Language", "en;q=0.5, zh-CN;q=1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	var envelope apiResponse
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Status != int(errDependencyUnavailable) {
		t.Fatalf("status = %d, want %d", envelope.Status, errDependencyUnavailable)
	}
	if envelope.ErrorMessage != "必要的服务依赖当前不可用。" {
		t.Fatalf("error_message = %q", envelope.ErrorMessage)
	}
	if strings.Contains(response.Body.String(), "database password must not leak") {
		t.Fatal("internal dependency error leaked to the response")
	}
}
