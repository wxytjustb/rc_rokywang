package mcpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"

	"notification-delivery/internal/application/notification"
	"notification-delivery/internal/authn"
	"notification-delivery/internal/bootstrap"
)

type staticOAuthHandler struct {
	token *oauth2.Token
}

func (h staticOAuthHandler) TokenSource(context.Context) (oauth2.TokenSource, error) {
	return oauth2.StaticTokenSource(h.token), nil
}

func (staticOAuthHandler) Authorize(context.Context, *http.Request, *http.Response) error {
	return nil
}

func TestStreamableHTTPListsToolsAndCallsCapabilityTool(t *testing.T) {
	t.Setenv("LARK_BOT_WEBHOOK_URL", "https://open.larksuite.com/open-apis/bot/v2/hook/test")
	registry, err := bootstrap.BuildRegistry("../../config/providers.yaml", nil, nil)
	if err != nil {
		t.Fatalf("BuildRegistry() error = %v", err)
	}
	handler := NewHandler(
		notification.NewService(nil, registry, nil, nil),
		authn.NewVerifier([]string{"shared-token"}),
		nil,
		Options{MaxBodyBytes: 1024 * 1024},
	)
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()

	unauthorized, err := http.Post(httpServer.URL, "application/json", http.NoBody)
	if err != nil {
		t.Fatalf("unauthorized POST error = %v", err)
	}
	_ = unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want 401", unauthorized.StatusCode)
	}
	authorizedRequest, err := http.NewRequest(http.MethodPost, httpServer.URL, http.NoBody)
	if err != nil {
		t.Fatalf("build authorized request: %v", err)
	}
	authorizedRequest.Header.Set("Authorization", "Bearer shared-token")
	authorized, err := http.DefaultClient.Do(authorizedRequest)
	if err != nil {
		t.Fatalf("authorized POST error = %v", err)
	}
	_ = authorized.Body.Close()
	if authorized.StatusCode == http.StatusUnauthorized {
		t.Fatal("authorized request was rejected by server bearer middleware")
	}

	client := mcp.NewClient(&mcp.Implementation{Name: "notification-test", Version: "1.0.0"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:             httpServer.URL,
		DisableStandaloneSSE: true,
		OAuthHandler:         staticOAuthHandler{token: &oauth2.Token{AccessToken: "shared-token"}},
	}, nil)
	if err != nil {
		t.Fatalf("MCP Connect() error = %v", err)
	}
	defer session.Close()

	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}
	if len(tools.Tools) != 3 {
		t.Fatalf("tool count = %d, want 3", len(tools.Tools))
	}
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "list_provider_capabilities",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool() error = %v", err)
	}
	if result.IsError || result.StructuredContent == nil {
		t.Fatalf("CallTool() result = %+v, want structured success", result)
	}
	invalidSubmit, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "submit_notification",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("invalid submit CallTool() protocol error = %v", err)
	}
	if !invalidSubmit.IsError {
		t.Fatalf("invalid submit result = %+v, want tool error", invalidSubmit)
	}
}
