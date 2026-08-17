package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"notification-delivery/internal/bootstrap"
	"notification-delivery/internal/provider"
)

func TestListProvidersReturnsRuntimeCapabilities(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("LARK_BOT_WEBHOOK_URL", "https://open.larksuite.com/open-apis/bot/v2/hook/test")
	registry, err := bootstrap.BuildRegistry("../../config/providers.yaml", nil, nil)
	if err != nil {
		t.Fatalf("BuildRegistry() error = %v", err)
	}
	router := NewRouter(Deps{
		Registry:   registry,
		AuthTokens: []string{"dev-system-token"},
	})

	request := httptest.NewRequest(http.MethodGet, "/v1/providers", nil)
	request.Header.Set("Authorization", "Bearer dev-system-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET /v1/providers status = %d, want %d", response.Code, http.StatusOK)
	}

	var envelope struct {
		Status int                      `json:"status"`
		Data   providerCapabilitiesData `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Status != 0 {
		t.Fatalf("response status = %d, want 0", envelope.Status)
	}
	if len(envelope.Data.Providers) != 1 {
		t.Fatalf("providers = %+v, want one provider", envelope.Data.Providers)
	}
	got := envelope.Data.Providers[0]
	if got.ProviderCode != "lark-bot" || len(got.Actions) != 1 {
		t.Fatalf("provider = %+v, want lark-bot with one action", got)
	}
	if got.Actions[0].ProviderAction != "send" {
		t.Fatalf("provider action = %q, want send", got.Actions[0].ProviderAction)
	}
	if got.Actions[0].Description != "向机器人所在群会话发送文本、富文本、图片、群名片或消息卡片；非幂等" {
		t.Fatalf("provider description = %q", got.Actions[0].Description)
	}
}

func TestCreateMessageReturnsLarkPayloadValidationDetails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("LARK_BOT_WEBHOOK_URL", "https://open.larksuite.com/open-apis/bot/v2/hook/test")
	registry, err := bootstrap.BuildRegistry("../../config/providers.yaml", nil, nil)
	if err != nil {
		t.Fatalf("BuildRegistry() error = %v", err)
	}
	router := NewRouter(Deps{
		Registry:   registry,
		AuthTokens: []string{"dev-system-token"},
	})

	body := []byte(`{
		"source_system":"example-system",
		"source_request_id":"request-1",
		"provider_code":"lark-bot",
		"provider_action":"send",
		"payload":{}
	}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev-system-token")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("POST /v1/messages status = %d, want %d; body = %s", response.Code, http.StatusUnprocessableEntity, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "payload.msg_type must be one of") {
		t.Fatalf("validation response does not identify invalid msg_type: %s", response.Body.String())
	}
}

func TestBuildProviderCapabilitiesSortsProvidersAndActions(t *testing.T) {
	resolved := map[string]provider.ResolvedAction{
		"z-provider/send": {
			Context:     provider.ActionContext{ProviderCode: "z-provider", ProviderAction: "send"},
			Description: "发送",
		},
		"a-provider/update": {
			Context:     provider.ActionContext{ProviderCode: "a-provider", ProviderAction: "update"},
			Description: "更新",
		},
		"a-provider/create": {
			Context:     provider.ActionContext{ProviderCode: "a-provider", ProviderAction: "create"},
			Description: "创建",
		},
	}

	got := buildProviderCapabilities(resolved)
	if len(got.Providers) != 2 || got.Providers[0].ProviderCode != "a-provider" || got.Providers[1].ProviderCode != "z-provider" {
		t.Fatalf("provider order = %+v", got.Providers)
	}
	actions := got.Providers[0].Actions
	if len(actions) != 2 || actions[0].ProviderAction != "create" || actions[1].ProviderAction != "update" {
		t.Fatalf("action order = %+v", actions)
	}
}
