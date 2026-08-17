package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfigFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadServerMemoryRuntime(t *testing.T) {
	t.Setenv("SWAGGER_ENABLED", "true")
	t.Setenv("AUTH_TOKEN", "dev-system-token")
	t.Setenv("DB_AUTO_CREATE", "true")
	path := writeConfigFile(t, `
swagger:
  enabled: ${SWAGGER_ENABLED:-false}
auth:
  tokens:
    - "${AUTH_TOKEN:-}"
database:
  auto_create: ${DB_AUTO_CREATE:-false}
mq:
  driver: memory
  memory:
    buffer_size: 128
    default_requeue_delay: 250ms
    max_requeue_delay: 2s
    max_attempts: 4
worker:
  lease:
    duration: 30s
  concurrency: 7
  compensator:
    publish_scan_interval: 2s
  http_client:
    max_response_bytes: 4096
`)

	cfg, err := LoadServer(path)
	if err != nil {
		t.Fatalf("LoadServer() error = %v", err)
	}
	if cfg.MQ.Driver != "memory" || cfg.MQ.Memory.BufferSize != 128 ||
		cfg.MQ.Memory.DefaultRequeueDelay != 250*time.Millisecond ||
		cfg.MQ.Memory.MaxRequeueDelay != 2*time.Second || cfg.MQ.Memory.MaxAttempts != 4 {
		t.Fatalf("memory config = %+v", cfg.MQ)
	}
	if cfg.Worker.Concurrency != 7 || cfg.Worker.Lease.Duration != 30*time.Second {
		t.Fatalf("worker runtime config = %+v", cfg.Worker)
	}
	if !cfg.Swagger.Enabled {
		t.Fatal("swagger.enabled = false, want true from SWAGGER_ENABLED")
	}
	if len(cfg.Auth.Tokens) != 1 || cfg.Auth.Tokens[0] != "dev-system-token" {
		t.Fatalf("auth.tokens = %q, want [dev-system-token]", cfg.Auth.Tokens)
	}
	if !cfg.Database.AutoCreate {
		t.Fatal("database.auto_create = false, want true from DB_AUTO_CREATE")
	}
}

func TestResolveAuthTokensUsesConfiguredTokens(t *testing.T) {
	tokens, generated, err := ResolveAuthTokens([]string{" token-a ", "", "token-b", "token-a"})
	if err != nil {
		t.Fatalf("ResolveAuthTokens() error = %v", err)
	}
	if generated != "" {
		t.Fatalf("generated token = %q, want empty", generated)
	}
	if len(tokens) != 2 || tokens[0] != "token-a" || tokens[1] != "token-b" {
		t.Fatalf("tokens = %q, want [token-a token-b]", tokens)
	}
}

func TestResolveAuthTokensGeneratesTokenWhenUnconfigured(t *testing.T) {
	tokens, generated, err := ResolveAuthTokens([]string{"", "  "})
	if err != nil {
		t.Fatalf("ResolveAuthTokens() error = %v", err)
	}
	if generated == "" {
		t.Fatal("generated token is empty")
	}
	if len(tokens) != 1 || tokens[0] != generated {
		t.Fatalf("tokens = %q, generated = %q", tokens, generated)
	}
}

func TestLoadWorkerKeepsRuntimeFieldsInline(t *testing.T) {
	path := writeConfigFile(t, `
mq:
  driver: nsq
  nsq:
    default_requeue_delay: 5s
    max_requeue_delay: 1m
    max_attempts: 7
  rabbitmq:
    default_requeue_delay: 2s
    max_requeue_delay: 30s
    max_attempts: 9
lease:
  duration: 45s
concurrency: 6
compensator:
  lease_scan_interval: 3s
http_client:
  max_response_bytes: 8192
`)

	cfg, err := LoadWorker(path)
	if err != nil {
		t.Fatalf("LoadWorker() error = %v", err)
	}
	if cfg.Concurrency != 6 || cfg.Lease.Duration != 45*time.Second {
		t.Fatalf("inline worker runtime config = %+v", cfg.WorkerRuntimeConfig)
	}
	if cfg.MQ.NSQ.DefaultRequeueDelay != 5*time.Second || cfg.MQ.NSQ.MaxRequeueDelay != time.Minute || cfg.MQ.NSQ.MaxAttempts != 7 {
		t.Fatalf("nsq requeue config = %+v", cfg.MQ.NSQ)
	}
	if cfg.MQ.RabbitMQ.DefaultRequeueDelay != 2*time.Second || cfg.MQ.RabbitMQ.MaxRequeueDelay != 30*time.Second || cfg.MQ.RabbitMQ.MaxAttempts != 9 {
		t.Fatalf("rabbitmq requeue config = %+v", cfg.MQ.RabbitMQ)
	}
}
