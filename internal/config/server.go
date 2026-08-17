package config

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

// ServerConfig is the root of server.yaml.
type ServerConfig struct {
	HTTP          HTTPConfig          `yaml:"http"`
	Swagger       SwaggerConfig       `yaml:"swagger"`
	Database      DatabaseConfig      `yaml:"database"`
	MQ            MQConfig            `yaml:"mq"`
	Worker        WorkerRuntimeConfig `yaml:"worker"`
	Auth          AuthConfig          `yaml:"auth"`
	ProvidersFile string              `yaml:"providers_file"`
	AutoMigrate   bool                `yaml:"auto_migrate"`
}

type SwaggerConfig struct {
	Enabled bool `yaml:"enabled"`
}

type HTTPConfig struct {
	Addr            string        `yaml:"addr"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	MaxBodyBytes    int64         `yaml:"max_body_bytes"`
}

type DatabaseConfig struct {
	Driver     string `yaml:"driver"`
	DSN        string `yaml:"dsn"`
	AutoCreate bool   `yaml:"auto_create"`
	MaxConns   int32  `yaml:"max_conns"`
	MinConns   int32  `yaml:"min_conns"`
}

// AuthConfig contains the bearer tokens accepted by the API. Tokens only
// authenticate API access; they are not associated with a source_system.
type AuthConfig struct {
	Tokens []string `yaml:"tokens"`
}

// ResolveAuthTokens returns the configured non-empty tokens. If none are
// configured, it creates one cryptographically random token and returns it in
// generatedToken so the caller can make it visible in the startup log.
func ResolveAuthTokens(configured []string) (tokens []string, generatedToken string, err error) {
	seen := make(map[string]struct{}, len(configured))
	for _, configuredToken := range configured {
		token := strings.TrimSpace(configuredToken)
		if token == "" {
			continue
		}
		if _, exists := seen[token]; exists {
			continue
		}
		seen[token] = struct{}{}
		tokens = append(tokens, token)
	}
	if len(tokens) > 0 {
		return tokens, "", nil
	}

	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return nil, "", fmt.Errorf("generate auth token: %w", err)
	}
	generatedToken = base64.RawURLEncoding.EncodeToString(random)
	return []string{generatedToken}, generatedToken, nil
}

// LoadServer reads and parses server.yaml.
func LoadServer(path string) (*ServerConfig, error) {
	var cfg ServerConfig
	if err := loadYAML(path, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
