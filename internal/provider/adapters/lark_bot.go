package adapters

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
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
	larkBotProviderCode = "lark-bot"
	actionSend          = "send"
	larkMaxRequestBytes = 20 * 1024
)

// LarkBot sends messages through a Lark (飞书) custom bot webhook. The
// webhook URL itself is a credential. When the bot enables signature
// verification, this adapter also adds Lark's documented timestamp and
// HMAC-SHA256 signature.
//
// Success is not the HTTP status line alone: a Lark webhook can return HTTP
// 200 and encode the real result in a {"code": ...} envelope. Only code=0 is
// success; every other response or transport error is retried by MQ policy.
type LarkBot struct{}

func (LarkBot) ProviderCode() string {
	return larkBotProviderCode
}

type larkBotConfig struct {
	SigningSecretRef string `yaml:"signing_secret_ref"`
	Actions          struct {
		Send larkSendConfig `yaml:"send"`
	} `yaml:"actions"`
}

type larkSendConfig struct {
	WebhookURL        string                    `yaml:"webhook_url"`
	TimeoutMs         int                       `yaml:"timeout_ms"`
	RequestsPerSecond float64                   `yaml:"requests_per_second"`
	MaxConcurrency    int                       `yaml:"max_concurrency"`
	CircuitBreaker    *larkCircuitBreakerConfig `yaml:"circuit_breaker"`
}

type larkCircuitBreakerConfig struct {
	FailureThreshold uint32        `yaml:"failure_threshold"`
	OpenDuration     time.Duration `yaml:"open_duration"`
}

func (LarkBot) Config(raw yaml.Node) (provider.Config, error) {
	var cfg larkBotConfig
	if err := provider.DecodeConfig(raw, &cfg); err != nil {
		return provider.Config{}, err
	}
	action := cfg.Actions.Send
	if err := validateLarkWebhookURL(action.WebhookURL); err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s.webhook_url: %w", actionSend, err)
	}
	if err := validateDeliveryLimits(action.TimeoutMs, action.RequestsPerSecond, action.MaxConcurrency); err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s: %w", actionSend, err)
	}
	if action.RequestsPerSecond > 5 {
		return provider.Config{}, fmt.Errorf("actions.%s.requests_per_second must not exceed Lark's documented 5 requests/second limit", actionSend)
	}
	breaker, err := normalizeLarkCircuitBreaker(action.CircuitBreaker)
	if err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s.circuit_breaker: %w", actionSend, err)
	}
	return provider.Config{
		CredentialRef: cfg.SigningSecretRef,
		Actions: map[string]provider.ActionConfig{
			actionSend: {
				Description:       "向机器人所在群会话发送文本、富文本、图片、群名片或消息卡片；非幂等",
				Path:              action.WebhookURL,
				Method:            "POST",
				TimeoutMs:         action.TimeoutMs,
				RequestsPerSecond: action.RequestsPerSecond,
				MaxConcurrency:    action.MaxConcurrency,
				CircuitBreaker:    breaker,
			},
		},
	}, nil
}

func normalizeLarkCircuitBreaker(cfg *larkCircuitBreakerConfig) (*provider.CircuitBreakerConfig, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.FailureThreshold == 0 {
		return nil, fmt.Errorf("failure_threshold must be greater than zero")
	}
	if cfg.OpenDuration <= 0 {
		return nil, fmt.Errorf("open_duration must be greater than zero")
	}
	return &provider.CircuitBreakerConfig{
		FailureThreshold: cfg.FailureThreshold,
		OpenDuration:     cfg.OpenDuration,
	}, nil
}

func validateLarkWebhookURL(raw string) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("must be an absolute URL")
	}
	if u.Scheme != "https" {
		loopback := u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"
		if u.Scheme != "http" || !loopback {
			return fmt.Errorf("must use HTTPS (HTTP is allowed only for a loopback test endpoint)")
		}
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("must not contain user info, query parameters, or a fragment")
	}
	const webhookPath = "/open-apis/bot/v2/hook/"
	if !strings.HasPrefix(u.Path, webhookPath) || strings.TrimPrefix(u.Path, webhookPath) == "" {
		return fmt.Errorf("path must match %s<webhook-id>", webhookPath)
	}
	return nil
}

func (LarkBot) Validate(action string, payload json.RawMessage) error {
	switch action {
	case actionSend:
		_, err := normalizeLarkMessage(payload)
		if err != nil {
			return &provider.ValidationError{Problems: []string{err.Error()}}
		}
		return nil
	default:
		return fmt.Errorf("lark_bot: unsupported action %q", action)
	}
}

func (a LarkBot) SendActionRequest(ctx context.Context, ac provider.ActionContext, action string, payload json.RawMessage) (*provider.Result, error) {
	switch action {
	case actionSend:
		return a.sendMessage(ctx, ac, payload)
	default:
		return nil, fmt.Errorf("lark_bot: unsupported action %q", action)
	}
}

// larkWebhookRequest follows Lark's documented custom-bot webhook body.
// Content is used by text/post/image/share_chat; Card by interactive.
type larkWebhookRequest struct {
	Timestamp string          `json:"timestamp,omitempty"`
	Sign      string          `json:"sign,omitempty"`
	MsgType   string          `json:"msg_type"`
	Content   json.RawMessage `json:"content,omitempty"`
	Card      json.RawMessage `json:"card,omitempty"`
}

type larkPayloadEnvelope struct {
	MsgType string          `json:"msg_type"`
	Content json.RawMessage `json:"content"`
	Card    json.RawMessage `json:"card"`
	// Text preserves the original {"text":"..."} caller contract. The
	// normalized request sent to Lark still uses msg_type/content.
	Text string `json:"text"`
}

// Code is a pointer so an undocumented body such as {} cannot be mistaken
// for the documented code=0 success response.
type larkWebhookResponse struct {
	Code *int   `json:"code"`
	Msg  string `json:"msg"`
}

func (LarkBot) sendMessage(ctx context.Context, ac provider.ActionContext, payload json.RawMessage) (*provider.Result, error) {
	bodyJSON, err := buildLarkRequestBody(payload, ac.Credential, time.Now())
	if err != nil {
		return nil, fmt.Errorf("build request body: %w", err)
	}
	if ac.HTTPClient == nil {
		return nil, fmt.Errorf("provider HTTP client is nil")
	}

	req := httpclient.Request{
		Method:  ac.Method,
		URL:     ac.Path,
		Headers: map[string]string{"Content-Type": "application/json; charset=utf-8"},
		Body:    bodyJSON,
	}

	resp, providerResponse, err := provider.Send(ctx, ac, req)
	if err != nil {
		// An explicit response followed by local response processing failure, or
		// worker cancellation, is not evidence that Lark is unavailable.
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
	result := interpretLarkResponse(resp)
	result.ProviderResponse = providerResponse
	return result, nil
}

func buildLarkRequestBody(payload json.RawMessage, signingSecret string, now time.Time) ([]byte, error) {
	message, err := normalizeLarkMessage(payload)
	if err != nil {
		return nil, err
	}
	if signingSecret != "" {
		timestamp := now.Unix()
		message.Timestamp = strconv.FormatInt(timestamp, 10)
		message.Sign = signLarkRequest(signingSecret, timestamp)
	}
	body, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	if len(body) > larkMaxRequestBytes {
		return nil, fmt.Errorf("Lark webhook request body is %d bytes; maximum is %d bytes", len(body), larkMaxRequestBytes)
	}
	return body, nil
}

func normalizeLarkMessage(payload json.RawMessage) (larkWebhookRequest, error) {
	if len(payload) > larkMaxRequestBytes {
		return larkWebhookRequest{}, fmt.Errorf("payload is %d bytes; Lark's maximum request body is %d bytes", len(payload), larkMaxRequestBytes)
	}
	var envelope larkPayloadEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return larkWebhookRequest{}, fmt.Errorf("payload must be a JSON object: %w", err)
	}

	// Backward compatibility for the original adapter contract.
	if envelope.MsgType == "" && strings.TrimSpace(envelope.Text) != "" {
		content, _ := json.Marshal(map[string]string{"text": envelope.Text})
		envelope.MsgType = "text"
		envelope.Content = content
	}

	message := larkWebhookRequest{MsgType: envelope.MsgType, Content: envelope.Content, Card: envelope.Card}
	switch envelope.MsgType {
	case "text":
		if err := requireLarkString(envelope.Content, "content", "text"); err != nil {
			return larkWebhookRequest{}, err
		}
	case "image":
		if err := requireLarkString(envelope.Content, "content", "image_key"); err != nil {
			return larkWebhookRequest{}, err
		}
	case "share_chat":
		if err := requireLarkString(envelope.Content, "content", "share_chat_id"); err != nil {
			return larkWebhookRequest{}, err
		}
	case "post":
		content, err := decodeLarkObject(envelope.Content, "content")
		if err != nil {
			return larkWebhookRequest{}, err
		}
		post, ok := content["post"]
		if !ok {
			return larkWebhookRequest{}, fmt.Errorf("payload.content.post is required for msg_type post")
		}
		locales, err := decodeLarkObject(post, "content.post")
		if err != nil {
			return larkWebhookRequest{}, err
		}
		if _, zh := locales["zh_cn"]; !zh {
			if _, en := locales["en_us"]; !en {
				return larkWebhookRequest{}, fmt.Errorf("payload.content.post must contain zh_cn or en_us")
			}
		}
	case "interactive":
		card, err := decodeLarkObject(envelope.Card, "card")
		if err != nil {
			return larkWebhookRequest{}, err
		}
		if len(card) == 0 {
			return larkWebhookRequest{}, fmt.Errorf("payload.card must not be empty for msg_type interactive")
		}
	default:
		return larkWebhookRequest{}, fmt.Errorf("payload.msg_type must be one of text, post, image, share_chat, interactive")
	}
	return message, nil
}

func requireLarkString(raw json.RawMessage, objectName, field string) error {
	object, err := decodeLarkObject(raw, objectName)
	if err != nil {
		return err
	}
	var value string
	if err := json.Unmarshal(object[field], &value); err != nil || strings.TrimSpace(value) == "" {
		return fmt.Errorf("payload.%s.%s must be a non-empty string", objectName, field)
	}
	return nil
}

func decodeLarkObject(raw json.RawMessage, name string) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, fmt.Errorf("payload.%s must be an object", name)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, fmt.Errorf("payload.%s must be an object", name)
	}
	return object, nil
}

func signLarkRequest(secret string, timestamp int64) string {
	stringToSign := strconv.FormatInt(timestamp, 10) + "\n" + secret
	mac := hmac.New(sha256.New, []byte(stringToSign))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func interpretLarkResponse(resp *httpclient.Response) *provider.Result {
	if resp.StatusCode != 200 {
		return &provider.Result{
			HTTPStatus:          resp.StatusCode,
			AvailabilityFailure: resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500,
			ErrorClass:          "LARK_HTTP_ERROR",
			Message:             fmt.Sprintf("lark webhook returned status %d", resp.StatusCode),
		}
	}

	var lr larkWebhookResponse
	if jsonErr := json.Unmarshal(resp.Body, &lr); jsonErr != nil {
		return &provider.Result{
			HTTPStatus:          resp.StatusCode,
			AvailabilityFailure: true,
			ErrorClass:          "UNEXPECTED_VENDOR_BODY",
			Message:             "lark webhook returned a non-JSON or unexpected body",
		}
	}
	if lr.Code == nil {
		return &provider.Result{
			HTTPStatus:          resp.StatusCode,
			AvailabilityFailure: true,
			ErrorClass:          "UNEXPECTED_VENDOR_BODY",
			Message:             "lark webhook response did not contain code",
		}
	}
	if *lr.Code == 0 {
		return &provider.Result{Success: true, HTTPStatus: resp.StatusCode}
	}
	return &provider.Result{
		HTTPStatus:          resp.StatusCode,
		AvailabilityFailure: *lr.Code == 11232,
		ErrorClass:          fmt.Sprintf("LARK_CODE_%d", *lr.Code),
		Message:             lr.Msg,
	}
}
