package adapters

import "fmt"

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
