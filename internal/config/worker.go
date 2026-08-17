package config

import "time"

// WorkerConfig is the root of worker.yaml.
type WorkerConfig struct {
	Database            DatabaseConfig `yaml:"database"`
	MQ                  MQConfig       `yaml:"mq"`
	ProvidersFile       string         `yaml:"providers_file"`
	WorkerRuntimeConfig `yaml:",inline"`
	AutoMigrate         bool `yaml:"auto_migrate"`
}

// WorkerRuntimeConfig contains the settings shared by the standalone worker
// and the worker embedded in cmd/server when mq.driver is "memory".
type WorkerRuntimeConfig struct {
	Lease       LeaseConfig       `yaml:"lease"`
	Concurrency int               `yaml:"concurrency"`
	Compensator CompensatorConfig `yaml:"compensator"`
	HTTPClient  HTTPClientConfig  `yaml:"http_client"`
}

type LeaseConfig struct {
	Duration      time.Duration `yaml:"duration"`
	RenewInterval time.Duration `yaml:"renew_interval"`
}

// CompensatorConfig drives the background loop, embedded in every worker
// process, that (a) publishes rows the API failed to enqueue after commit
// and (b) reclaims rows whose worker lease expired without a crash-free
// handoff.
type CompensatorConfig struct {
	PublishScanInterval time.Duration `yaml:"publish_scan_interval"`
	PublishBatchSize    int           `yaml:"publish_batch_size"`
	LeaseScanInterval   time.Duration `yaml:"lease_scan_interval"`
	LeaseScanBatchSize  int           `yaml:"lease_scan_batch_size"`
}

type HTTPClientConfig struct {
	MaxResponseBytes   int64    `yaml:"max_response_bytes"`
	AllowedRespHeaders []string `yaml:"allowed_response_headers"`
}

// LoadWorker reads and parses worker.yaml.
func LoadWorker(path string) (*WorkerConfig, error) {
	var cfg WorkerConfig
	if err := loadYAML(path, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
