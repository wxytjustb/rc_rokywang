package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"gopkg.in/yaml.v3"

	"notification-delivery/internal/httpclient"
	"notification-delivery/internal/provider"
)

const (
	loadtestHTTPProviderCode = "loadtest-http"
	loadtestActionRequest    = "request"
)

// LoadtestHTTP is a loopback-only adapter for isolated delivery benchmarks.
// It deliberately has no vendor protocol, credentials, circuit breaker, or
// requests-per-second cap: HTTP 2xx is explicit success and every other result
// follows the Worker's ordinary retry policy.
type LoadtestHTTP struct{}

func (LoadtestHTTP) ProviderCode() string { return loadtestHTTPProviderCode }

type loadtestHTTPConfig struct {
	Actions struct {
		Request loadtestHTTPRequestConfig `yaml:"request"`
	} `yaml:"actions"`
}

type loadtestHTTPRequestConfig struct {
	EndpointURL    string `yaml:"endpoint_url"`
	TimeoutMs      int    `yaml:"timeout_ms"`
	MaxConcurrency int    `yaml:"max_concurrency"`
}

func (LoadtestHTTP) Config(raw yaml.Node) (provider.Config, error) {
	var cfg loadtestHTTPConfig
	if err := provider.DecodeConfig(raw, &cfg); err != nil {
		return provider.Config{}, err
	}
	action := cfg.Actions.Request
	if err := validateLoadtestEndpoint(action.EndpointURL); err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s.endpoint_url: %w", loadtestActionRequest, err)
	}
	if action.TimeoutMs <= 0 {
		return provider.Config{}, fmt.Errorf("actions.%s.timeout_ms must be greater than zero", loadtestActionRequest)
	}
	if action.MaxConcurrency <= 0 {
		return provider.Config{}, fmt.Errorf("actions.%s.max_concurrency must be greater than zero", loadtestActionRequest)
	}
	return provider.Config{Actions: map[string]provider.ActionConfig{
		loadtestActionRequest: {
			Description:       "压测专用：向本机 HTTP 模拟后端发送 JSON 请求，以 HTTP 2xx 作为成功",
			Path:              action.EndpointURL,
			Method:            "POST",
			TimeoutMs:         action.TimeoutMs,
			RequestsPerSecond: 0, // No rate cap; Worker and action concurrency remain bounded.
			MaxConcurrency:    action.MaxConcurrency,
		},
	}}, nil
}

func validateLoadtestEndpoint(raw string) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("must be an absolute URL")
	}
	loopback := u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"
	if u.Scheme != "http" || !loopback {
		return fmt.Errorf("must use HTTP on a loopback host")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("must not contain user info, query parameters, or a fragment")
	}
	return nil
}

func (LoadtestHTTP) Validate(action string, payload json.RawMessage) error {
	if action != loadtestActionRequest {
		return fmt.Errorf("loadtest_http: unsupported action %q", action)
	}
	if !json.Valid(payload) {
		return &provider.ValidationError{Problems: []string{"payload must be valid JSON"}}
	}
	return nil
}

func (LoadtestHTTP) SendActionRequest(ctx context.Context, ac provider.ActionContext, action string, payload json.RawMessage) (*provider.Result, error) {
	if action != loadtestActionRequest {
		return nil, fmt.Errorf("loadtest_http: unsupported action %q", action)
	}
	resp, providerResponse, err := provider.Send(ctx, ac, httpclient.Request{
		Method:  ac.Method,
		URL:     ac.Path,
		Headers: map[string]string{"Content-Type": "application/json"},
		Body:    payload,
	})
	if err != nil {
		if resp != nil || context.Cause(ctx) != nil {
			return nil, fmt.Errorf("process loadtest response: %w", err)
		}
		errorClass := "LOADTEST_TRANSPORT_ERROR"
		if httpclient.PreSendFailure(err) {
			errorClass = "LOADTEST_PRE_SEND_CONNECT_FAILURE"
		}
		return &provider.Result{
			AvailabilityFailure: true,
			ErrorClass:          errorClass,
			Message:             err.Error(),
		}, nil
	}

	result := &provider.Result{
		Success:          resp.StatusCode >= 200 && resp.StatusCode < 300,
		HTTPStatus:       resp.StatusCode,
		ProviderResponse: providerResponse,
	}
	if !result.Success {
		result.AvailabilityFailure = resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500
		result.ErrorClass = "LOADTEST_HTTP_STATUS"
		result.Message = fmt.Sprintf("loadtest endpoint returned HTTP %d", resp.StatusCode)
	}
	return result, nil
}
