package api

import (
	"encoding/json"
	"time"
)

type createMessageRequest struct {
	SourceSystem    string          `json:"source_system" binding:"required" example:"example-system"`
	SourceRequestID string          `json:"source_request_id" binding:"required" example:"lark-bot-send-request-id-uuid4"`
	ProviderCode    string          `json:"provider_code" binding:"required" example:"lark-bot"`
	ProviderAction  string          `json:"provider_action" binding:"required" example:"send"`
	Payload         json.RawMessage `json:"payload" binding:"required" swaggertype:"object"`
}

// createLarkBotMessageExample is the executable Swagger example for the
// currently built-in provider action. Runtime decoding still uses
// createMessageRequest so future adapters may define different payloads.
type createLarkBotMessageExample struct {
	SourceSystem    string                `json:"source_system" example:"example-system"`
	SourceRequestID string                `json:"source_request_id" example:"lark-bot-send-request-id-uuid4"`
	ProviderCode    string                `json:"provider_code" example:"lark-bot"`
	ProviderAction  string                `json:"provider_action" example:"send"`
	Payload         larkBotMessageExample `json:"payload"`
}

type larkBotMessageExample struct {
	MsgType string                    `json:"msg_type" example:"text"`
	Content larkBotTextContentExample `json:"content"`
}

type larkBotTextContentExample struct {
	Text string `json:"text" example:"Notification Delivery test"`
}

type createMessageResponse struct {
	EventID         string    `json:"event_id" format:"uuid"`
	SourceSystem    string    `json:"source_system" example:"example-system"`
	SourceRequestID string    `json:"source_request_id" example:"lark-bot-send-request-id-uuid4"`
	Status          string    `json:"status" enums:"PENDING,PROCESSING,SUCCEEDED,FAILED,UNKNOWN" example:"PENDING"`
	Duplicate       bool      `json:"duplicate"`
	AcceptedAt      time.Time `json:"accepted_at"`
}

type messageStatusResponse struct {
	EventID          string          `json:"event_id" format:"uuid"`
	SourceSystem     string          `json:"source_system" example:"example-system"`
	SourceRequestID  string          `json:"source_request_id" example:"lark-bot-send-request-id-uuid4"`
	ProviderCode     string          `json:"provider_code" example:"lark-bot"`
	ProviderAction   string          `json:"provider_action" example:"send"`
	Status           string          `json:"status" enums:"PENDING,PROCESSING,SUCCEEDED,FAILED,UNKNOWN" example:"SUCCEEDED"`
	AttemptCount     int16           `json:"attempt_count"`
	LastResult       json.RawMessage `json:"last_result" swaggertype:"object"`
	ProviderResponse json.RawMessage `json:"provider_response" swaggertype:"object"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

type providerActionCapability struct {
	ProviderAction string `json:"provider_action" example:"send"`
	Description    string `json:"description" example:"向机器人所在群会话发送文本、富文本、图片、群名片或消息卡片；非幂等"`
}

type providerCapability struct {
	ProviderCode string                     `json:"provider_code" example:"lark-bot"`
	Actions      []providerActionCapability `json:"actions"`
}

type providerCapabilitiesData struct {
	Providers []providerCapability `json:"providers"`
}

type emptyData struct{}

type readinessData struct {
	Ready bool `json:"ready" example:"true"`
}

type apiResponse struct {
	Status       int    `json:"status" example:"0"`
	Data         any    `json:"data" swaggertype:"object"`
	ErrorMessage string `json:"error_message" example:""`
}

type emptyAPIResponse struct {
	Status       int       `json:"status" example:"0"`
	Data         emptyData `json:"data"`
	ErrorMessage string    `json:"error_message" example:""`
}

type readinessAPIResponse struct {
	Status       int           `json:"status" example:"0"`
	Data         readinessData `json:"data"`
	ErrorMessage string        `json:"error_message" example:""`
}

type createMessageAPIResponse struct {
	Status       int                   `json:"status" example:"0"`
	Data         createMessageResponse `json:"data"`
	ErrorMessage string                `json:"error_message" example:""`
}

type messageStatusAPIResponse struct {
	Status       int                   `json:"status" example:"0"`
	Data         messageStatusResponse `json:"data"`
	ErrorMessage string                `json:"error_message" example:""`
}

type providerCapabilitiesAPIResponse struct {
	Status       int                      `json:"status" example:"0"`
	Data         providerCapabilitiesData `json:"data"`
	ErrorMessage string                   `json:"error_message" example:""`
}

type errorResponse struct {
	Status       int       `json:"status" example:"1001"`
	Data         emptyData `json:"data"`
	ErrorMessage string    `json:"error_message" example:"The request body is invalid."`
}
