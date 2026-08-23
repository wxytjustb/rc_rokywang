package adapters

import "notification-delivery/internal/provider"

// Register wires every built-in example adapter into reg. Each adapter is
// keyed by its own ProviderCode() — providers.yaml needs no separate
// "adapter:" field, its top-level provider_code keys must simply match
// what these adapters declare.
func Register(reg *provider.Registry) {
	reg.RegisterAdapter(LarkBot{})
	reg.RegisterAdapter(NewSMTPEmail())
	reg.RegisterAdapter(NewWebhook())
	reg.RegisterAdapter(LoadtestHTTP{})
}
