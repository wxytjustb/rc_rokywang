package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Status is the lifecycle state of a notification_event row.
type Status string

const (
	StatusPending    Status = "PENDING"
	StatusProcessing Status = "PROCESSING"
	StatusFailed     Status = "FAILED"
)

// Event mirrors one row of notification_event.
type Event struct {
	ID               uuid.UUID
	SourceSystem     string
	SourceRequestID  string
	ProviderCode     string
	ProviderAction   string
	Payload          json.RawMessage
	Status           Status
	AttemptCount     int16
	NextAttemptAt    time.Time
	EnqueuedAt       *time.Time
	LeaseToken       *uuid.UUID
	LeaseUntil       *time.Time
	LastResult       json.RawMessage
	ProviderResponse json.RawMessage
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// LastResultPhase values used while an event is PROCESSING.
const (
	PhaseClaimed    = "CLAIMED"
	PhaseRequesting = "REQUESTING"
)

// LastResultClaimed is written the moment a worker wins the atomic claim.
type LastResultClaimed struct {
	Phase     string    `json:"phase"`
	ClaimedAt time.Time `json:"claimed_at"`
}

// LastResultRequesting is written right before the outbound HTTP call.
type LastResultRequesting struct {
	Phase     string    `json:"phase"`
	StartedAt time.Time `json:"started_at"`
}

// Outcome values used once a delivery attempt has finished. Every provider
// failure records REQUEUE_REQUESTED while the event remains PROCESSING; the
// selected MQ owns when it is delivered again.
const (
	OutcomeRequeueRequested = "REQUEUE_REQUESTED"
	OutcomeSucceeded        = "SUCCEEDED"
	OutcomeFailed           = "FAILED"
)

// LastResultFinished is written when an attempt finishes, including a
// transient result that schedules another attempt, or when recovery settles
// an expired lease.
type LastResultFinished struct {
	Outcome                string     `json:"outcome"`
	HTTPStatus             int        `json:"http_status,omitempty"`
	ErrorClass             string     `json:"error_class,omitempty"`
	Message                string     `json:"message,omitempty"`
	RequestMayHaveBeenSent bool       `json:"request_may_have_been_sent,omitempty"`
	Retryable              bool       `json:"retryable,omitempty"`
	NextAttemptAt          *time.Time `json:"next_attempt_at,omitempty"`
	LatencyMs              int64      `json:"latency_ms,omitempty"`
	AttemptNumber          int16      `json:"attempt_number"`
	FinishedAt             time.Time  `json:"finished_at"`
}

// ProviderResponse is the sanitized, size-bounded record of the vendor's
// most recent explicit HTTP response. Per DESIGN.md §3.1: binary bodies are
// never stored (ContentLength/BodyDigest instead of Body), only an allowed
// subset of headers is kept, and the body is truncated above a size cap.
type ProviderResponse struct {
	HTTPStatus    int               `json:"http_status"`
	ContentType   string            `json:"content_type,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Body          json.RawMessage   `json:"body,omitempty"`
	ContentLength int64             `json:"content_length,omitempty"`
	BodyDigest    string            `json:"body_digest,omitempty"`
	ReceivedAt    time.Time         `json:"received_at"`
	Truncated     bool              `json:"truncated"`
}
