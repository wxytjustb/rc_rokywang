package adapters

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"notification-delivery/internal/httpclient"
	"notification-delivery/internal/provider"
)

const (
	webhookProviderCode    = "webhook"
	webhookActionDeliver   = "deliver"
	webhookMaxPayloadBytes = 256 * 1024

	webhookAuthNone   = "none"
	webhookAuthBearer = "bearer"
	webhookAuthHMAC   = "hmac_sha256"
)

// Webhook delivers one JSON object to one operator-configured endpoint. The
// target URL is never accepted from the message payload, and redirects are
// rejected, keeping the destination inside the reviewed configuration
// boundary instead of turning this adapter into an arbitrary HTTP client.
type Webhook struct {
	authentication string
}

func NewWebhook() *Webhook {
	return &Webhook{}
}

func (*Webhook) ProviderCode() string {
	return webhookProviderCode
}

type webhookConfig struct {
	CredentialRef string `yaml:"credential_ref"`
	Actions       struct {
		Deliver webhookDeliverConfig `yaml:"deliver"`
	} `yaml:"actions"`
}

type webhookDeliverConfig struct {
	EndpointURL       string                       `yaml:"endpoint_url"`
	Authentication    string                       `yaml:"authentication"`
	TimeoutMs         int                          `yaml:"timeout_ms"`
	RequestsPerSecond float64                      `yaml:"requests_per_second"`
	MaxConcurrency    int                          `yaml:"max_concurrency"`
	CircuitBreaker    *adapterCircuitBreakerConfig `yaml:"circuit_breaker"`
}

func (w *Webhook) Config(raw yaml.Node) (provider.Config, error) {
	var cfg webhookConfig
	if err := provider.DecodeConfig(raw, &cfg); err != nil {
		return provider.Config{}, err
	}
	action := cfg.Actions.Deliver
	if err := validateFixedWebhookURL(action.EndpointURL); err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s.endpoint_url: %w", webhookActionDeliver, err)
	}
	if err := validateDeliveryLimits(action.TimeoutMs, action.RequestsPerSecond, action.MaxConcurrency); err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s: %w", webhookActionDeliver, err)
	}
	authentication := strings.ToLower(strings.TrimSpace(action.Authentication))
	if authentication == "" {
		authentication = webhookAuthNone
	}
	credentialRef := strings.TrimSpace(cfg.CredentialRef)
	switch authentication {
	case webhookAuthNone:
		if credentialRef != "" {
			return provider.Config{}, fmt.Errorf("credential_ref must be empty when authentication is none")
		}
	case webhookAuthBearer, webhookAuthHMAC:
		if credentialRef == "" {
			return provider.Config{}, fmt.Errorf("credential_ref is required when authentication is %s", authentication)
		}
	default:
		return provider.Config{}, fmt.Errorf("actions.%s.authentication must be none, bearer, or hmac_sha256", webhookActionDeliver)
	}
	breaker, err := normalizeCircuitBreaker(action.CircuitBreaker)
	if err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s.circuit_breaker: %w", webhookActionDeliver, err)
	}
	w.authentication = authentication
	return provider.Config{
		CredentialRef: credentialRef,
		Actions: map[string]provider.ActionConfig{
			webhookActionDeliver: {
				Description:       "将 JSON 对象投递到配置中固定的 Webhook 端点；支持 Bearer 或 HMAC-SHA256 认证，禁止重定向",
				Path:              action.EndpointURL,
				Method:            "POST",
				TimeoutMs:         action.TimeoutMs,
				RequestsPerSecond: action.RequestsPerSecond,
				MaxConcurrency:    action.MaxConcurrency,
				CircuitBreaker:    breaker,
			},
		},
	}, nil
}

func (*Webhook) Validate(action string, payload json.RawMessage) error {
	if action != webhookActionDeliver {
		return fmt.Errorf("webhook: unsupported action %q", action)
	}
	if err := validateWebhookPayload(payload); err != nil {
		return &provider.ValidationError{Problems: []string{err.Error()}}
	}
	return nil
}

func (w *Webhook) SendActionRequest(ctx context.Context, ac provider.ActionContext, action string, payload json.RawMessage) (*provider.Result, error) {
	if action != webhookActionDeliver {
		return nil, fmt.Errorf("webhook: unsupported action %q", action)
	}
	if err := validateWebhookPayload(payload); err != nil {
		return nil, fmt.Errorf("validate webhook payload: %w", err)
	}
	if ac.HTTPClient == nil {
		return nil, fmt.Errorf("provider HTTP client is nil")
	}

	headers := map[string]string{
		"Content-Type":    "application/json; charset=utf-8",
		"Idempotency-Key": webhookIdempotencyKey(ac.ProviderCode, ac.SourceSystem, ac.SourceRequestID),
	}
	authentication := w.authentication
	if authentication == "" {
		return nil, fmt.Errorf("webhook: adapter config was not initialized")
	}
	if authentication != webhookAuthNone && ac.Credential == "" {
		return nil, fmt.Errorf("webhook: resolved credential is empty")
	}
	switch authentication {
	case webhookAuthBearer:
		headers["Authorization"] = "Bearer " + ac.Credential
	case webhookAuthHMAC:
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		headers["X-Webhook-Timestamp"] = timestamp
		headers["X-Webhook-Signature"] = signWebhook(ac.Credential, timestamp, payload)
	case webhookAuthNone:
	default:
		return nil, fmt.Errorf("webhook: unsupported normalized authentication %q", authentication)
	}

	resp, providerResponse, err := provider.Send(ctx, ac, httpclient.Request{
		Method:          ac.Method,
		URL:             ac.Path,
		Headers:         headers,
		Body:            payload,
		RejectRedirects: true,
	})
	if err != nil {
		if resp != nil || context.Cause(ctx) != nil {
			return nil, fmt.Errorf("process provider response: %w", err)
		}
		errorClass := "TRANSPORT_ERROR_NO_RESPONSE"
		if httpclient.PreSendFailure(err) {
			errorClass = "PRE_SEND_CONNECT_FAILURE"
		}
		return &provider.Result{
			AvailabilityFailure: true,
			ErrorClass:          errorClass,
			Message:             err.Error(),
		}, nil
	}
	result := interpretWebhookResponse(resp)
	result.ProviderResponse = providerResponse
	return result, nil
}

func validateWebhookPayload(payload json.RawMessage) error {
	if len(payload) > webhookMaxPayloadBytes {
		return fmt.Errorf("payload is %d bytes; maximum is %d bytes", len(payload), webhookMaxPayloadBytes)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil || object == nil {
		return fmt.Errorf("payload must be a JSON object")
	}
	return nil
}

func validateFixedWebhookURL(raw string) error {
	u, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return fmt.Errorf("must be an absolute URL")
	}
	if u.Scheme != "https" {
		if u.Scheme != "http" || !isLoopbackHost(u.Hostname()) {
			return fmt.Errorf("must use HTTPS (HTTP is allowed only for a loopback test endpoint)")
		}
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("must not contain user info, query parameters, or a fragment")
	}
	return nil
}

func interpretWebhookResponse(resp *httpclient.Response) *provider.Result {
	result := &provider.Result{HTTPStatus: resp.StatusCode}
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		result.Success = true
		result.Message = "webhook endpoint explicitly accepted the request"
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		result.ErrorClass = "WEBHOOK_REDIRECT_REJECTED"
		result.Message = fmt.Sprintf("webhook endpoint returned redirect status %d", resp.StatusCode)
	case resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500:
		result.AvailabilityFailure = true
		result.ErrorClass = "WEBHOOK_HTTP_ERROR"
		result.Message = fmt.Sprintf("webhook endpoint returned HTTP status %d", resp.StatusCode)
	default:
		result.ErrorClass = "WEBHOOK_HTTP_ERROR"
		result.Message = fmt.Sprintf("webhook endpoint returned HTTP status %d", resp.StatusCode)
	}
	return result
}

func webhookIdempotencyKey(providerCode, sourceSystem, sourceRequestID string) string {
	sum := sha256.Sum256([]byte(providerCode + "\x00" + sourceSystem + "\x00" + sourceRequestID))
	return hex.EncodeToString(sum[:])
}

func signWebhook(secret, timestamp string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
