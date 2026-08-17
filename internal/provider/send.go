package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"notification-delivery/internal/httpclient"
)

// Send performs the mechanical part shared by provider adapters: apply the
// configured timeout, execute the request, and sanitize the bounded response.
// Interpreting a vendor's explicit success envelope remains in its adapter.
func Send(ctx context.Context, ac ActionContext, req httpclient.Request) (*httpclient.Response, json.RawMessage, error) {
	if ac.HTTPClient == nil {
		return nil, nil, fmt.Errorf("provider HTTP client is nil")
	}
	if req.Timeout <= 0 {
		req.Timeout = time.Duration(ac.TimeoutMs) * time.Millisecond
	}
	resp, err := ac.HTTPClient.Do(ctx, req)
	if err != nil {
		return resp, nil, err
	}
	providerResponse, err := buildProviderResponse(resp, ac.AllowedRespHeaders)
	if err != nil {
		return resp, nil, fmt.Errorf("sanitize provider response: %w", err)
	}
	return resp, providerResponse, nil
}
