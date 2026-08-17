package config

import "time"

// MQConfig selects and configures exactly one delivery backend. NSQ and
// RabbitMQ are durable external brokers. Memory is an in-process wake-up
// queue used when the API server embeds its worker runtime.
type MQConfig struct {
	Driver   string         `yaml:"driver"` // "memory", "nsq" or "rabbitmq"
	Memory   MemoryConfig   `yaml:"memory"`
	NSQ      NSQConfig      `yaml:"nsq"`
	RabbitMQ RabbitMQConfig `yaml:"rabbitmq"`
}

// RequeueConfig is shared by every consumer implementation. MaxAttempts=0
// means unbounded provider calls; positive values include the first real
// provider call. Delay after a failed call is min(default*attempts, max).
// Broker delivery attempts are transport metadata and do not enforce this limit.
type RequeueConfig struct {
	DefaultRequeueDelay time.Duration `yaml:"default_requeue_delay"`
	MaxRequeueDelay     time.Duration `yaml:"max_requeue_delay"`
	MaxAttempts         uint32        `yaml:"max_attempts"`
}

type MemoryConfig struct {
	BufferSize    int `yaml:"buffer_size"`
	RequeueConfig `yaml:",inline"`
}

type NSQConfig struct {
	// NSQDAddr is used by the publisher to write directly to an nsqd.
	NSQDAddr string `yaml:"nsqd_addr"`
	// LookupdAddrs is used by the consumer to discover nsqd instances.
	LookupdAddrs  []string `yaml:"lookupd_addrs"`
	Topic         string   `yaml:"topic"`
	Channel       string   `yaml:"channel"`
	RequeueConfig `yaml:",inline"`
}

type RabbitMQConfig struct {
	URL            string        `yaml:"url"`
	Exchange       string        `yaml:"exchange"`
	ExchangeType   string        `yaml:"exchange_type"`
	Queue          string        `yaml:"queue"`
	RoutingKey     string        `yaml:"routing_key"`
	Durable        bool          `yaml:"durable"`
	PrefetchCount  int           `yaml:"prefetch_count"`
	ReconnectDelay time.Duration `yaml:"reconnect_delay"`
	RequeueConfig  `yaml:",inline"`
}
