package adapters

import (
	"fmt"
	"time"

	"notification-delivery/internal/provider"
)

type adapterCircuitBreakerConfig struct {
	FailureThreshold uint32        `yaml:"failure_threshold"`
	OpenDuration     time.Duration `yaml:"open_duration"`
}

// validateDeliveryLimits covers the process-level safety bounds shared by
// every adapter's Config method: per-action timeout and throughput caps.
func validateDeliveryLimits(timeoutMs int, requestsPerSecond float64, maxConcurrency int) error {
	if timeoutMs <= 0 {
		return fmt.Errorf("timeout_ms must be greater than zero")
	}
	if requestsPerSecond <= 0 {
		return fmt.Errorf("requests_per_second must be greater than zero")
	}
	if maxConcurrency <= 0 {
		return fmt.Errorf("max_concurrency must be greater than zero")
	}
	return nil
}

func normalizeCircuitBreaker(cfg *adapterCircuitBreakerConfig) (*provider.CircuitBreakerConfig, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.FailureThreshold == 0 {
		return nil, fmt.Errorf("failure_threshold must be greater than zero")
	}
	if cfg.OpenDuration <= 0 {
		return nil, fmt.Errorf("open_duration must be greater than zero")
	}
	return &provider.CircuitBreakerConfig{
		FailureThreshold: cfg.FailureThreshold,
		OpenDuration:     cfg.OpenDuration,
	}, nil
}
