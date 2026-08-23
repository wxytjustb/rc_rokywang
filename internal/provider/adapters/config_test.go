package adapters

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	appconfig "notification-delivery/internal/config"
	"notification-delivery/internal/provider"
)

type testCredentialResolver struct{}

func (testCredentialResolver) Resolve(ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	return "test-secret", nil
}

func TestLarkBotConfigNormalizesAction(t *testing.T) {
	cfg, err := LarkBot{}.Config(configNode(t, `
signing_secret_ref: vault://notification/lark-bot-signing-secret
actions:
  send:
    webhook_url: https://open.feishu.cn/open-apis/bot/v2/hook/test
    timeout_ms: 5000
    requests_per_second: 5
    max_concurrency: 5
    circuit_breaker:
      failure_threshold: 5
      open_duration: 30s
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	action, ok := cfg.Actions[actionSend]
	if !ok {
		t.Fatalf("Config() actions = %#v, want %q", cfg.Actions, actionSend)
	}
	if action.Method != "POST" {
		t.Fatalf("normalized method = %q, want %q", action.Method, "POST")
	}
	if cfg.CredentialRef != "vault://notification/lark-bot-signing-secret" {
		t.Fatalf("credential ref = %q", cfg.CredentialRef)
	}
	if action.CircuitBreaker == nil || action.CircuitBreaker.FailureThreshold != 5 || action.CircuitBreaker.OpenDuration != 30*time.Second {
		t.Fatalf("normalized circuit breaker = %+v", action.CircuitBreaker)
	}
}

func TestLarkBotConfigOmittedCircuitBreakerDisablesIt(t *testing.T) {
	cfg, err := LarkBot{}.Config(configNode(t, `
actions:
  send:
    webhook_url: https://open.feishu.cn/open-apis/bot/v2/hook/test
    timeout_ms: 5000
    requests_per_second: 5
    max_concurrency: 5
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	if got := cfg.Actions[actionSend].CircuitBreaker; got != nil {
		t.Fatalf("omitted circuit breaker = %+v, want nil", got)
	}
}

func TestLarkBotConfigRejectsInvalidCircuitBreaker(t *testing.T) {
	for _, body := range []string{
		`failure_threshold: 0
      open_duration: 30s`,
		`failure_threshold: 5
      open_duration: 0s`,
	} {
		_, err := LarkBot{}.Config(configNode(t, `
actions:
  send:
    webhook_url: https://open.larksuite.com/open-apis/bot/v2/hook/test
    timeout_ms: 5000
    requests_per_second: 5
    max_concurrency: 5
    circuit_breaker:
      `+body+`
`))
		if err == nil || !strings.Contains(err.Error(), "circuit_breaker") {
			t.Fatalf("Config() error = %v, want circuit-breaker validation error", err)
		}
	}
}

func TestLarkBotConfigRejectsRateAboveDocumentedLimit(t *testing.T) {
	_, err := LarkBot{}.Config(configNode(t, `
actions:
  send:
    webhook_url: https://open.larksuite.com/open-apis/bot/v2/hook/test
    timeout_ms: 5000
    requests_per_second: 5.1
    max_concurrency: 5
`))
	if err == nil || !strings.Contains(err.Error(), "5 requests/second") {
		t.Fatalf("Config() error = %v, want documented rate-limit error", err)
	}
}

func TestAdapterConfigRejectsCrossAdapterFields(t *testing.T) {
	_, err := LarkBot{}.Config(configNode(t, `
credential_ref: vault://bearer-token
actions:
  send:
    webhook_url: https://open.feishu.cn/open-apis/bot/v2/hook/test
`))
	if err == nil || !strings.Contains(err.Error(), "field credential_ref not found") {
		t.Fatalf("Config() error = %v, want strict unknown-field error", err)
	}
}

func TestLarkBotConfigRejectsRetryField(t *testing.T) {
	_, err := LarkBot{}.Config(configNode(t, `
actions:
  send:
    webhook_url: https://open.feishu.cn/open-apis/bot/v2/hook/test
    timeout_ms: 5000
    requests_per_second: 5
    max_concurrency: 5
    retry:
      max_attempts: 3
`))
	if err == nil || !strings.Contains(err.Error(), "field retry not found") {
		t.Fatalf("Config() error = %v, want provider retry field rejection", err)
	}
}

func TestRepositoryProvidersConfigBuildsThroughAdapterConfigs(t *testing.T) {
	t.Setenv("LARK_BOT_WEBHOOK_URL", "https://open.larksuite.com/open-apis/bot/v2/hook/test")
	cfg, err := appconfig.LoadProviders(filepath.Join("..", "..", "..", "config", "providers.yaml"))
	if err != nil {
		t.Fatalf("LoadProviders() error = %v", err)
	}
	registry := provider.NewRegistry(cfg, testCredentialResolver{}, nil, nil)
	Register(registry)
	if err := registry.Build(); err != nil {
		t.Fatalf("Registry.Build() error = %v", err)
	}

	resolved, ok := registry.Lookup(larkBotProviderCode, actionSend)
	if !ok {
		t.Fatalf("Lookup(%q, %q) not found", larkBotProviderCode, actionSend)
	}
	if resolved.Context.Method != "POST" {
		t.Fatalf("Lookup(%q, %q) method = %q, want %q", larkBotProviderCode, actionSend, resolved.Context.Method, "POST")
	}
}

func TestRepositoryLoadtestProvidersConfigBuildsIndependently(t *testing.T) {
	cfg, err := appconfig.LoadProviders(filepath.Join("..", "..", "..", "config", "providers.loadtest.yaml"))
	if err != nil {
		t.Fatalf("LoadProviders() error = %v", err)
	}
	registry := provider.NewRegistry(cfg, testCredentialResolver{}, nil, nil)
	Register(registry)
	if err := registry.Build(); err != nil {
		t.Fatalf("Registry.Build() error = %v", err)
	}

	resolved, ok := registry.Lookup(loadtestHTTPProviderCode, loadtestActionRequest)
	if !ok {
		t.Fatalf("Lookup(%q, %q) not found", loadtestHTTPProviderCode, loadtestActionRequest)
	}
	if resolved.RequestsPerSecond != 0 || resolved.MaxConcurrency != 256 {
		t.Fatalf("loadtest limits = rps %v concurrency %d", resolved.RequestsPerSecond, resolved.MaxConcurrency)
	}
	if _, ok := registry.Lookup(larkBotProviderCode, actionSend); ok {
		t.Fatal("loadtest-only registry unexpectedly contains lark-bot/send")
	}
}

func TestRepositoryP0ProvidersExampleBuilds(t *testing.T) {
	cfg, err := appconfig.LoadProviders(filepath.Join("..", "..", "..", "config", "providers.p0.example.yaml"))
	if err != nil {
		t.Fatalf("LoadProviders() error = %v", err)
	}
	registry := provider.NewRegistry(cfg, testCredentialResolver{}, nil, nil)
	Register(registry)
	if err := registry.Build(); err != nil {
		t.Fatalf("Registry.Build() error = %v", err)
	}
	if _, ok := registry.Lookup(smtpEmailProviderCode, smtpActionSend); !ok {
		t.Fatalf("Lookup(%q, %q) not found", smtpEmailProviderCode, smtpActionSend)
	}
	if _, ok := registry.Lookup(webhookProviderCode, webhookActionDeliver); !ok {
		t.Fatalf("Lookup(%q, %q) not found", webhookProviderCode, webhookActionDeliver)
	}
}

func TestRegisterIncludesSMTPEmailAndWebhookAdapters(t *testing.T) {
	cfg := &appconfig.ProvidersConfig{Providers: map[string]yaml.Node{
		smtpEmailProviderCode: configNode(t, `
password_ref: vault://notification/smtp-password
actions:
  send:
    host: smtp.example.com
    port: 587
    tls_mode: starttls
    username: notifier@example.com
    from_address: notifier@example.com
    timeout_ms: 5000
    requests_per_second: 10
    max_concurrency: 4
`),
		webhookProviderCode: configNode(t, `
credential_ref: vault://notification/webhook-secret
actions:
  deliver:
    endpoint_url: https://events.example.com/notifications
    authentication: bearer
    timeout_ms: 5000
    requests_per_second: 20
    max_concurrency: 10
`),
	}}
	registry := provider.NewRegistry(cfg, testCredentialResolver{}, nil, nil)
	Register(registry)
	if err := registry.Build(); err != nil {
		t.Fatalf("Registry.Build() error = %v", err)
	}
	if _, ok := registry.Lookup(smtpEmailProviderCode, smtpActionSend); !ok {
		t.Fatalf("Lookup(%q, %q) not found", smtpEmailProviderCode, smtpActionSend)
	}
	if _, ok := registry.Lookup(webhookProviderCode, webhookActionDeliver); !ok {
		t.Fatalf("Lookup(%q, %q) not found", webhookProviderCode, webhookActionDeliver)
	}
}

func configNode(t *testing.T, text string) yaml.Node {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(text), &document); err != nil {
		t.Fatalf("parse test YAML: %v", err)
	}
	if len(document.Content) != 1 {
		t.Fatalf("test YAML has %d document nodes, want 1", len(document.Content))
	}
	return *document.Content[0]
}
