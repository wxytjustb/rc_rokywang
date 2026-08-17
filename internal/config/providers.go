package config

import "gopkg.in/yaml.v3"

// ProvidersConfig is the root of providers.yaml. It routes each raw provider
// block to the adapter with the matching provider_code; the adapter owns that
// block's schema and validation rules.
type ProvidersConfig struct {
	// Provider configuration is intentionally kept as raw YAML here: not
	// every vendor's config shares one honest config schema (a bearer-auth
	// REST API and a Lark webhook, for instance, look nothing alike). The
	// registered adapter for each provider_code owns decoding and
	// validating its node through Adapter.Config.
	Providers map[string]yaml.Node `yaml:"providers"`
}

// LoadProviders reads and parses providers.yaml.
func LoadProviders(path string) (*ProvidersConfig, error) {
	var cfg ProvidersConfig
	if err := loadYAML(path, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
