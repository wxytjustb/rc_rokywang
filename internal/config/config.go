// Package config loads YAML configuration for the server and worker
// binaries. File contents go through os.ExpandEnv before parsing so secrets
// (DSNs, broker credentials) can be injected via environment variables
// instead of being committed to the YAML files themselves.
package config

import (
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// envRef matches ${NAME} and ${NAME:-default}, the two forms config files
// may use to pull a value from the environment.
var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

func expandEnv(s string) string {
	return envRef.ReplaceAllStringFunc(s, func(match string) string {
		groups := envRef.FindStringSubmatch(match)
		name, hasDefault, def := groups[1], groups[2] != "", groups[3]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		if hasDefault {
			return def
		}
		return ""
	})
}

func loadYAML(path string, out interface{}) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	expanded := expandEnv(string(raw))
	if err := yaml.Unmarshal([]byte(expanded), out); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}
	return nil
}
