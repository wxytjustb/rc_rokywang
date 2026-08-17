// Package provider implements the Provider Adapter Registry described in
// DESIGN.md §5: a fixed, code-reviewed mapping from provider_code to an
// Adapter that owns payload validation, vendor HTTP request construction,
// sending and explicit-success detection for every action it supports. There
// is no dynamic scripting — adding a vendor action means writing (or
// extending) an Adapter and registering it.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"notification-delivery/internal/httpclient"
)

// Result reports whether the provider explicitly confirmed one delivery.
// Every non-success result is retryable by policy; the worker derives delay
// and the maximum real-call decision from the persisted attempt count.
type Result struct {
	Success bool
	// AvailabilityFailure is true only when this result indicates that the
	// provider action is temporarily unavailable. It affects the circuit
	// breaker only; every non-success result still follows the MQ retry policy.
	AvailabilityFailure bool
	// HTTPStatus is 0 if no response was ever received (e.g. a pre-send
	// connect failure or a request timeout).
	HTTPStatus int
	// ErrorClass is a short machine-readable label, e.g. "READ_TIMEOUT".
	ErrorClass string
	Message    string
	// ProviderResponse is the already-sanitized, size-bounded JSON record
	// of the vendor's response (DESIGN.md §3.1), ready to store as-is in
	// notification_event.provider_response. Nil if no explicit response
	// was received for this attempt.
	ProviderResponse json.RawMessage
}

// ActionContext carries the resolved, non-secret configuration for one
// provider_code + provider_action pair, the resolved credential value an
// adapter needs to authenticate the call, and the shared infrastructure
// (HTTP client, response-header allow-list) every adapter sends through.
type ActionContext struct {
	ProviderCode   string
	ProviderAction string
	BaseURL        string
	Path           string
	Method         string
	Credential     string
	TimeoutMs      int
	// SourceRequestID is set by the worker from the event row before
	// calling SendActionRequest. An adapter may forward it to the vendor
	// (header or body) purely to aid the vendor's own support/reconciliation
	// tooling — per DESIGN.md §7 this is never treated as the vendor
	// actually implementing idempotency.
	SourceRequestID string

	// HTTPClient is the shared, size/timeout-bounded client (DESIGN.md
	// §11.1) adapters must send vendor requests through. It is nil when
	// the registry was built for validate-only use (the API server never
	// calls SendActionRequest, so it never dereferences this).
	HTTPClient *httpclient.Client
	// AllowedRespHeaders is the header allow-list applied when sanitizing
	// the vendor response into ProviderResponse.
	AllowedRespHeaders []string
}

// Config is the normalized runtime result of an adapter's provider-specific
// configuration rules. It is deliberately not the shape of providers.yaml:
// every adapter decodes that YAML into its own private config type and then
// returns only the common values the delivery runtime needs.
type Config struct {
	CredentialRef string
	Actions       map[string]ActionConfig
}

// ActionConfig is the adapter-normalized runtime configuration for one
// action. Protocol rules such as method and URL construction are decided
// by the owning adapter's Config method, not by a global YAML schema.
type ActionConfig struct {
	Description       string
	BaseURL           string
	Path              string
	Method            string
	TimeoutMs         int
	RequestsPerSecond float64
	MaxConcurrency    int
	CircuitBreaker    *CircuitBreakerConfig
}

// CircuitBreakerConfig is the normalized runtime policy produced by an
// adapter's private configuration schema. Nil disables the breaker.
type CircuitBreakerConfig struct {
	FailureThreshold uint32
	OpenDuration     time.Duration
}

// DecodeConfig strictly decodes one provider's raw YAML node into the
// adapter's private config type. KnownFields makes misspelled or
// cross-adapter fields a startup error instead of silently ignoring them.
func DecodeConfig(raw yaml.Node, out any) error {
	encoded, err := yaml.Marshal(&raw)
	if err != nil {
		return fmt.Errorf("encode provider config: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("decode provider config: %w", err)
	}
	return nil
}

// Adapter is the fixed conversion logic and configuration authority for one
// provider_code (DESIGN.md §5). The registry looks it up by ProviderCode()
// directly against the providers.yaml top-level key.
type Adapter interface {
	// ProviderCode returns the provider_code (the top-level key under
	// "providers:" in providers.yaml) this adapter implements, e.g.
	// "lark-bot".
	ProviderCode() string

	// Config owns this adapter's complete configuration schema and rules.
	// Implementations strictly decode raw into an adapter-private config type,
	// validate vendor-specific invariants, and normalize enabled actions for
	// the registry. Different adapters are free to expose entirely different
	// YAML shapes.
	Config(raw yaml.Node) (Config, error)

	// Validate checks payload against action's input requirements.
	Validate(action string, payload json.RawMessage) error

	// SendActionRequest builds and sends the vendor-specific request. Success
	// must be true only when the vendor explicitly confirms the action. A
	// returned error and every result with Success=false are both requeued by
	// the worker until the persisted provider-attempt limit is exhausted.
	SendActionRequest(ctx context.Context, ac ActionContext, action string, payload json.RawMessage) (*Result, error)
}
