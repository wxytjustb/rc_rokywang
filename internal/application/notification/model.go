package notification

import (
	"encoding/json"
	"time"
)

type SubmitCommand struct {
	SourceSystem    string
	SourceRequestID string
	ProviderCode    string
	ProviderAction  string
	Payload         json.RawMessage
}

type SubmitResult struct {
	EventID         string
	SourceSystem    string
	SourceRequestID string
	Status          string
	Duplicate       bool
	AcceptedAt      time.Time
}

type StatusQuery struct {
	SourceSystem    string
	SourceRequestID string
}

type ProviderActionCapability struct {
	ProviderAction string
	Description    string
}

type ProviderCapability struct {
	ProviderCode string
	Actions      []ProviderActionCapability
}
