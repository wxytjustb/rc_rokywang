// Package bootstrap holds the startup wiring shared by cmd/server and
// cmd/worker so the provider registry can never drift between the two
// processes.
package bootstrap

import (
	"fmt"

	"notification-delivery/internal/config"
	"notification-delivery/internal/httpclient"
	"notification-delivery/internal/provider"
	"notification-delivery/internal/provider/adapters"
)

// BuildRegistry loads providers.yaml and resolves it against the built-in
// adapters. It fails fast (before serving any traffic or consuming any MQ
// message) if a provider references an unregistered adapter, an
// unresolvable credential_ref, or an action its adapter doesn't claim to
// support.
//
// httpClient and allowedRespHeaders are threaded into every resolved
// action for adapters' SendActionRequest to use. Pass httpClient as nil
// for a validate-only registry — the API server only ever calls Validate,
// never SendActionRequest, so it does not need one.
func BuildRegistry(providersFile string, httpClient *httpclient.Client, allowedRespHeaders []string) (*provider.Registry, error) {
	providersCfg, err := config.LoadProviders(providersFile)
	if err != nil {
		return nil, fmt.Errorf("load providers config: %w", err)
	}
	reg := provider.NewRegistry(providersCfg, provider.EnvCredentialResolver{}, httpClient, allowedRespHeaders)
	adapters.Register(reg)
	if err := reg.Build(); err != nil {
		return nil, fmt.Errorf("build provider registry: %w", err)
	}
	return reg, nil
}
