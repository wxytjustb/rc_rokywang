package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestSwaggerUIAndDocumentedRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(Deps{
		SwaggerEnabled: true,
		AuthTokens:     []string{"dev-system-token"},
	})

	redirectResponse := httptest.NewRecorder()
	router.ServeHTTP(redirectResponse, httptest.NewRequest(http.MethodGet, "/docs", nil))
	if redirectResponse.Code != http.StatusTemporaryRedirect {
		t.Fatalf("GET /docs status = %d, want %d", redirectResponse.Code, http.StatusTemporaryRedirect)
	}
	if location := redirectResponse.Header().Get("Location"); location != "/docs/index.html" {
		t.Fatalf("GET /docs Location = %q, want %q", location, "/docs/index.html")
	}

	uiResponse := httptest.NewRecorder()
	router.ServeHTTP(uiResponse, httptest.NewRequest(http.MethodGet, "/docs/index.html", nil))
	if uiResponse.Code != http.StatusOK {
		t.Fatalf("GET /docs/index.html status = %d, want %d", uiResponse.Code, http.StatusOK)
	}
	if !strings.Contains(uiResponse.Body.String(), "SwaggerUIBundle") {
		t.Fatal("GET /docs/index.html did not return Swagger UI")
	}
	if !strings.Contains(uiResponse.Body.String(), `const defaultBearerToken = "dev-system-token"`) {
		t.Fatal("GET /docs/index.html does not contain the default bearer token")
	}
	if !strings.Contains(uiResponse.Body.String(), `ui.preauthorizeApiKey("BearerAuth", defaultBearerToken)`) {
		t.Fatal("GET /docs/index.html does not preauthorize BearerAuth")
	}

	docResponse := httptest.NewRecorder()
	router.ServeHTTP(docResponse, httptest.NewRequest(http.MethodGet, "/docs/doc.json", nil))
	if docResponse.Code != http.StatusOK {
		t.Fatalf("GET /docs/doc.json status = %d, want %d", docResponse.Code, http.StatusOK)
	}

	var document struct {
		Paths               map[string]json.RawMessage `json:"paths"`
		SecurityDefinitions map[string]json.RawMessage `json:"securityDefinitions"`
		Definitions         map[string]struct {
			Properties map[string]struct {
				Type    string `json:"type"`
				Example any    `json:"example"`
				Ref     string `json:"$ref"`
			} `json:"properties"`
		} `json:"definitions"`
	}
	if err := json.Unmarshal(docResponse.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode Swagger document: %v", err)
	}

	for _, path := range []string{
		"/healthz",
		"/readyz",
		"/v1/providers",
		"/v1/messages",
		"/v1/messages/{source_request_id}",
	} {
		if _, ok := document.Paths[path]; !ok {
			t.Errorf("Swagger document is missing API path %q", path)
		}
	}
	if _, ok := document.SecurityDefinitions["BearerAuth"]; !ok {
		t.Error("Swagger document is missing BearerAuth security definition")
	}
	requestDefinition := document.Definitions["api.createLarkBotMessageExample"]
	wantExamples := map[string]string{
		"provider_action":   "send",
		"provider_code":     "lark-bot",
		"source_request_id": "lark-bot-send-request-id-uuid4",
		"source_system":     "example-system",
	}
	for property, want := range wantExamples {
		if got, _ := requestDefinition.Properties[property].Example.(string); got != want {
			t.Errorf("Swagger request example %s = %q, want %q", property, got, want)
		}
	}
	if got := requestDefinition.Properties["payload"].Ref; got != "#/definitions/api.larkBotMessageExample" {
		t.Errorf("Swagger request payload ref = %q", got)
	}
	larkPayload := document.Definitions["api.larkBotMessageExample"]
	if got, _ := larkPayload.Properties["msg_type"].Example.(string); got != "text" {
		t.Errorf("Swagger payload msg_type example = %q, want text", got)
	}
	if got := larkPayload.Properties["content"].Ref; got != "#/definitions/api.larkBotTextContentExample" {
		t.Errorf("Swagger payload content ref = %q", got)
	}
	textContent := document.Definitions["api.larkBotTextContentExample"]
	if got, _ := textContent.Properties["text"].Example.(string); got != "Notification Delivery test" {
		t.Errorf("Swagger payload text example = %q", got)
	}
	for _, definitionName := range []string{
		"api.emptyAPIResponse",
		"api.readinessAPIResponse",
		"api.createMessageAPIResponse",
		"api.messageStatusAPIResponse",
		"api.providerCapabilitiesAPIResponse",
		"api.errorResponse",
	} {
		definition := document.Definitions[definitionName]
		for _, property := range []string{"status", "data", "error_message"} {
			if _, ok := definition.Properties[property]; !ok {
				t.Errorf("Swagger definition %s is missing %s", definitionName, property)
			}
		}
	}
}

func TestSwaggerRoutesDisabledByDefault(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(Deps{})

	for _, path := range []string{"/docs", "/docs/index.html", "/docs/doc.json", "/swagger/index.html"} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want %d", path, response.Code, http.StatusNotFound)
		}
	}
}
