package worker

import (
	"context"
	"time"

	"notification-delivery/internal/domain"
	"notification-delivery/internal/provider"
)

func (p *Processor) logProviderRequest(ctx context.Context, ev *domain.Event, ac provider.ActionContext, result *provider.Result, latency time.Duration) {
	if p.Logger == nil {
		return
	}

	status, failureReason := providerRequestStatus(result)
	attrs := []any{
		"event_id", ev.ID.String(),
		"source_system", ev.SourceSystem,
		"source_request_id", ev.SourceRequestID,
		"attempt_number", ev.AttemptCount,
		"provider_code", ev.ProviderCode,
		"provider_action", ev.ProviderAction,
		"timeout_ms", ac.TimeoutMs,
		"payload_bytes", len(ev.Payload),
		"status", status,
		"latency_ms", latency.Milliseconds(),
		"provider_response_bytes", len(result.ProviderResponse),
	}
	if failureReason != "" {
		attrs = append(attrs, "failure_reason", failureReason)
	}
	p.Logger.InfoContext(ctx, "provider request completed", attrs...)
}

func providerRequestStatus(result *provider.Result) (string, string) {
	if result.Success {
		return "SUCCESS", ""
	}
	return "RETRYABLE_FAILURE", failureReason(result)
}

func failureReason(result *provider.Result) string {
	reason := result.ErrorClass
	if reason == "" {
		reason = "PROVIDER_ERROR"
	}
	return reason
}
