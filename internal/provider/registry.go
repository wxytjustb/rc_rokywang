package provider

import (
	"fmt"

	"notification-delivery/internal/config"
	"notification-delivery/internal/httpclient"
)

// CredentialResolver turns a credential_ref (e.g. "vault://notification/lark-bot")
// from providers.yaml into the actual secret value. Production deployments
// back this with a real secrets manager; config files only ever hold the
// reference, never the secret (DESIGN.md §5, §11.1).
type CredentialResolver interface {
	Resolve(ref string) (string, error)
}

// ResolvedAction is one provider_code+provider_action entry from
// providers.yaml joined with the Adapter implementation for its vendor and
// its resolved credential.
type ResolvedAction struct {
	Adapter           Adapter
	Context           ActionContext
	Description       string
	TimeoutMs         int
	RequestsPerSecond float64
	MaxConcurrency    int
	CircuitBreaker    *CircuitBreakerConfig
}

// Registry resolves (provider_code, provider_action) to an Adapter plus its
// configuration. It is built once at startup from providers.yaml and the
// set of compiled-in adapters; there is no runtime mutation, matching the
// "no arbitrary script execution" boundary in DESIGN.md §5.
type Registry struct {
	cfg                *config.ProvidersConfig
	adapters           map[string]Adapter // keyed by Adapter.ProviderCode()
	resolver           CredentialResolver
	httpClient         *httpclient.Client
	allowedRespHeaders []string
	resolved           map[string]ResolvedAction // keyed by "provider_code/provider_action"
}

// NewRegistry builds an empty Registry. httpClient and allowedRespHeaders
// are threaded into every resolved ActionContext for adapters'
// SendActionRequest to use; pass httpClient as nil for a validate-only
// registry (the API server never calls SendActionRequest, only Validate).
func NewRegistry(cfg *config.ProvidersConfig, resolver CredentialResolver, httpClient *httpclient.Client, allowedRespHeaders []string) *Registry {
	return &Registry{
		cfg:                cfg,
		adapters:           make(map[string]Adapter),
		resolver:           resolver,
		httpClient:         httpClient,
		allowedRespHeaders: allowedRespHeaders,
		resolved:           make(map[string]ResolvedAction),
	}
}

// RegisterAdapter makes an Adapter available under the provider_code it
// declares via ProviderCode() — that value must match a top-level key
// under "providers:" in providers.yaml for Build to find it.
func (r *Registry) RegisterAdapter(a Adapter) {
	r.adapters[a.ProviderCode()] = a
}

// Build asks every registered adapter to decode and validate its own raw YAML
// node, then resolves credentials and normalizes actions for the worker. Call
// once after all RegisterAdapter calls so malformed or cross-adapter config is
// caught at startup rather than on the first delivery attempt.
func (r *Registry) Build() error {
	for providerCode, raw := range r.cfg.Providers {
		adapter, ok := r.adapters[providerCode]
		if !ok {
			return fmt.Errorf("provider %s: no adapter registered for this provider_code", providerCode)
		}
		pc, err := adapter.Config(raw)
		if err != nil {
			return fmt.Errorf("provider %s: invalid adapter config: %w", providerCode, err)
		}
		if len(pc.Actions) == 0 {
			return fmt.Errorf("provider %s: adapter config enables no actions", providerCode)
		}
		credential, err := r.resolver.Resolve(pc.CredentialRef)
		if err != nil {
			return fmt.Errorf("provider %s: resolve credential_ref %q: %w", providerCode, pc.CredentialRef, err)
		}
		for actionName, ac := range pc.Actions {
			key := key(providerCode, actionName)
			r.resolved[key] = ResolvedAction{
				Adapter: adapter,
				Context: ActionContext{
					ProviderCode:       providerCode,
					ProviderAction:     actionName,
					BaseURL:            ac.BaseURL,
					Path:               ac.Path,
					Method:             ac.Method,
					Credential:         credential,
					TimeoutMs:          ac.TimeoutMs,
					HTTPClient:         r.httpClient,
					AllowedRespHeaders: r.allowedRespHeaders,
				},
				Description:       ac.Description,
				TimeoutMs:         ac.TimeoutMs,
				RequestsPerSecond: ac.RequestsPerSecond,
				MaxConcurrency:    ac.MaxConcurrency,
				CircuitBreaker:    ac.CircuitBreaker,
			}
		}
	}
	return nil
}

// Lookup returns the resolved adapter and config for provider_code +
// provider_action, or ok=false if the combination is not supported — the
// API maps that to HTTP 422 with numeric status 1004 per DESIGN.md §4.1.
func (r *Registry) Lookup(providerCode, providerAction string) (ResolvedAction, bool) {
	ra, ok := r.resolved[key(providerCode, providerAction)]
	return ra, ok
}

// All returns every resolved action, e.g. for the worker to size
// per-action rate limiters at startup.
func (r *Registry) All() map[string]ResolvedAction {
	return r.resolved
}

func key(providerCode, providerAction string) string {
	return providerCode + "/" + providerAction
}
