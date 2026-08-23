package mcpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"notification-delivery/internal/application/notification"
)

type submitInput struct {
	SourceSystem    string         `json:"source_system" jsonschema:"required,the stable source system name"`
	SourceRequestID string         `json:"source_request_id" jsonschema:"required,the source-generated idempotency key"`
	ProviderCode    string         `json:"provider_code" jsonschema:"required,the runtime-enabled provider code"`
	ProviderAction  string         `json:"provider_action" jsonschema:"required,the runtime-enabled provider action"`
	Payload         map[string]any `json:"payload" jsonschema:"required,the provider-owned JSON payload"`
}

type submitOutput struct {
	EventID         string `json:"event_id"`
	SourceSystem    string `json:"source_system"`
	SourceRequestID string `json:"source_request_id"`
	Status          string `json:"status"`
	Duplicate       bool   `json:"duplicate"`
	AcceptedAt      string `json:"accepted_at"`
}

type getStatusInput struct {
	SourceSystem    string `json:"source_system" jsonschema:"required,the source system name used when submitting"`
	SourceRequestID string `json:"source_request_id" jsonschema:"required,the source request id used when submitting"`
}

type getStatusOutput struct {
	EventID          string `json:"event_id"`
	SourceSystem     string `json:"source_system"`
	SourceRequestID  string `json:"source_request_id"`
	ProviderCode     string `json:"provider_code"`
	ProviderAction   string `json:"provider_action"`
	Status           string `json:"status"`
	AttemptCount     int16  `json:"attempt_count"`
	LastResult       any    `json:"last_result,omitempty"`
	ProviderResponse any    `json:"provider_response,omitempty"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
}

type listCapabilitiesInput struct{}

type providerActionOutput struct {
	ProviderAction string `json:"provider_action"`
	Description    string `json:"description"`
}

type providerOutput struct {
	ProviderCode string                 `json:"provider_code"`
	Actions      []providerActionOutput `json:"actions"`
}

type listCapabilitiesOutput struct {
	Providers []providerOutput `json:"providers"`
}

func registerTools(server *mcp.Server, service *notification.Service) {
	readOnly := true
	nonDestructive := false
	closedWorld := false
	openWorld := true

	mcp.AddTool(server, &mcp.Tool{
		Name:        "submit_notification",
		Title:       "Submit notification",
		Description: "Durably accepts a notification for asynchronous delivery. Success means accepted and persisted, not delivered by the provider.",
		Annotations: &mcp.ToolAnnotations{
			DestructiveHint: &nonDestructive,
			IdempotentHint:  true,
			OpenWorldHint:   &openWorld,
		},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input submitInput) (*mcp.CallToolResult, submitOutput, error) {
		payload, err := json.Marshal(input.Payload)
		if err != nil {
			return nil, submitOutput{}, fmt.Errorf("INVALID_REQUEST: payload cannot be encoded")
		}
		result, err := service.Submit(ctx, notification.SubmitCommand{
			SourceSystem:    input.SourceSystem,
			SourceRequestID: input.SourceRequestID,
			ProviderCode:    input.ProviderCode,
			ProviderAction:  input.ProviderAction,
			Payload:         payload,
		})
		if err != nil {
			return nil, submitOutput{}, toolError(err)
		}
		return nil, submitOutput{
			EventID:         result.EventID,
			SourceSystem:    result.SourceSystem,
			SourceRequestID: result.SourceRequestID,
			Status:          result.Status,
			Duplicate:       result.Duplicate,
			AcceptedAt:      result.AcceptedAt.Format(time.RFC3339Nano),
		}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_notification_status",
		Title:       "Get notification status",
		Description: "Returns the persisted asynchronous delivery state for one source request.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:  readOnly,
			OpenWorldHint: &closedWorld,
		},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input getStatusInput) (*mcp.CallToolResult, getStatusOutput, error) {
		event, err := service.GetStatus(ctx, notification.StatusQuery{
			SourceSystem:    input.SourceSystem,
			SourceRequestID: input.SourceRequestID,
		})
		if err != nil {
			return nil, getStatusOutput{}, toolError(err)
		}
		return nil, getStatusOutput{
			EventID:          event.ID.String(),
			SourceSystem:     event.SourceSystem,
			SourceRequestID:  event.SourceRequestID,
			ProviderCode:     event.ProviderCode,
			ProviderAction:   event.ProviderAction,
			Status:           string(event.Status),
			AttemptCount:     event.AttemptCount,
			LastResult:       decodeJSON(event.LastResult),
			ProviderResponse: decodeJSON(event.ProviderResponse),
			CreatedAt:        event.CreatedAt.Format(time.RFC3339Nano),
			UpdatedAt:        event.UpdatedAt.Format(time.RFC3339Nano),
		}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_provider_capabilities",
		Title:       "List provider capabilities",
		Description: "Lists provider codes and actions enabled by the current runtime configuration.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:  readOnly,
			OpenWorldHint: &closedWorld,
		},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ listCapabilitiesInput) (*mcp.CallToolResult, listCapabilitiesOutput, error) {
		capabilities := service.ListCapabilities(ctx)
		providers := make([]providerOutput, 0, len(capabilities))
		for _, capability := range capabilities {
			actions := make([]providerActionOutput, 0, len(capability.Actions))
			for _, action := range capability.Actions {
				actions = append(actions, providerActionOutput{
					ProviderAction: action.ProviderAction,
					Description:    action.Description,
				})
			}
			providers = append(providers, providerOutput{ProviderCode: capability.ProviderCode, Actions: actions})
		}
		return nil, listCapabilitiesOutput{Providers: providers}, nil
	})
}

func decodeJSON(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	return value
}

func toolError(err error) error {
	var validationErr *notification.PayloadValidationError
	switch {
	case errors.Is(err, notification.ErrInvalidRequest):
		return errors.New("INVALID_REQUEST: required fields or payload are invalid")
	case errors.Is(err, notification.ErrUnsupportedProviderAction):
		return errors.New("UNSUPPORTED_PROVIDER_ACTION: provider or action is not enabled")
	case errors.As(err, &validationErr):
		message := "INVALID_PAYLOAD: payload does not meet provider requirements"
		if len(validationErr.Problems) > 0 {
			message += ": " + strings.Join(validationErr.Problems, "; ")
		}
		return errors.New(message)
	case errors.Is(err, notification.ErrInvalidPayload):
		return errors.New("INVALID_PAYLOAD: payload does not meet provider requirements")
	case errors.Is(err, notification.ErrSourceRequestConflict):
		return errors.New("SOURCE_REQUEST_CONFLICT: the idempotency key was previously accepted with different content")
	case errors.Is(err, notification.ErrNotFound):
		return errors.New("NOT_FOUND: notification was not found")
	case errors.Is(err, notification.ErrStorageUnavailable):
		return errors.New("STORAGE_UNAVAILABLE: notification storage is temporarily unavailable")
	default:
		return errors.New("INTERNAL: notification operation failed")
	}
}
