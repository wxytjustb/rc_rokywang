// Package mq defines the message-queue boundary described in DESIGN.md §4.5
// and §13: the wire format is a single JSON object carrying only event_id,
// and the broker product itself (NSQ or RabbitMQ) must stay swappable
// without touching the rest of the system. Callers depend only on
// Publisher/Consumer; concrete drivers live in nsq.go and rabbitmq.go.
package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"notification-delivery/internal/config"
)

// envelope is the entire MQ message body. No business fields ride along;
// the consumer always re-reads current state from PostgreSQL by id.
type envelope struct {
	EventID string `json:"event_id"`
}

func encode(id uuid.UUID) ([]byte, error) {
	return json.Marshal(envelope{EventID: id.String()})
}

func decode(body []byte) (uuid.UUID, error) {
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return uuid.UUID{}, fmt.Errorf("decode mq envelope: %w", err)
	}
	id, err := uuid.Parse(env.EventID)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("decode mq envelope event_id: %w", err)
	}
	return id, nil
}

// PublishResult describes the delivery guarantee obtained from a successful
// Publish call. Durable is true only when an external broker acknowledged
// persistence; an in-process wake-up cannot make that promise.
type PublishResult struct {
	Durable bool
}

// Publisher hands an event_id to the selected delivery backend.
type Publisher interface {
	Publish(ctx context.Context, eventID uuid.UUID) (PublishResult, error)
	Close() error
}

// Delivery carries broker-owned delivery metadata and the configured retry
// policy. Attempts is retained for transport observability only. The database
// event attempt_count is authoritative for provider attempts and terminal
// max_attempts decisions.
type Delivery struct {
	EventID      uuid.UUID
	Attempts     uint32
	MaxAttempts  uint32
	RequeueDelay time.Duration

	defaultRequeueDelay time.Duration
	maxRequeueDelay     time.Duration
}

// RequeueDelayFor calculates retry timing from a real provider-attempt
// number, independent of how many times the broker transported the envelope.
func (d Delivery) RequeueDelayFor(attempt uint32) time.Duration {
	if d.defaultRequeueDelay > 0 {
		return linearRequeueDelay(attempt, d.defaultRequeueDelay, d.maxRequeueDelay)
	}
	return d.RequeueDelay
}

// RequeueError lets Worker request an exact retry delay. Backoff is meaningful
// to NSQ: ordinary provider failures participate in go-nsq backoff, while an
// open circuit uses a per-message delay without slowing unrelated handlers.
type RequeueError struct {
	Cause   error
	Delay   time.Duration
	Backoff bool
}

func (e *RequeueError) Error() string {
	if e == nil || e.Cause == nil {
		return "mq: requeue requested"
	}
	return e.Cause.Error()
}

func (e *RequeueError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// RequestRequeue creates the typed handler result consumed by every MQ
// backend. Negative delays are normalized to zero.
func RequestRequeue(cause error, delay time.Duration, backoff bool) error {
	if delay < 0 {
		delay = 0
	}
	return &RequeueError{Cause: cause, Delay: delay, Backoff: backoff}
}

func requestedRequeue(err error, fallback time.Duration) (time.Duration, bool, bool) {
	var request *RequeueError
	if errors.As(err, &request) {
		return request.Delay, request.Backoff, true
	}
	return fallback, true, false
}

// linearRequeueDelay implements the shared MQ policy:
// min(default_requeue_delay * attempts, max_requeue_delay).
func linearRequeueDelay(attempts uint32, defaultDelay, maxDelay time.Duration) time.Duration {
	if attempts == 0 {
		attempts = 1
	}
	if defaultDelay <= 0 {
		return 0
	}
	if maxDelay > 0 {
		// Check the cap before multiplying so a corrupt/unbounded attempt
		// counter cannot overflow time.Duration.
		if time.Duration(attempts) > maxDelay/defaultDelay {
			return maxDelay
		}
	}
	delay := defaultDelay * time.Duration(attempts)
	if maxDelay > 0 && delay > maxDelay {
		return maxDelay
	}
	return delay
}

func validateRequeueConfig(driver string, cfg config.RequeueConfig) error {
	if cfg.DefaultRequeueDelay <= 0 {
		return fmt.Errorf("mq: %s default_requeue_delay must be greater than zero", driver)
	}
	if cfg.MaxRequeueDelay < cfg.DefaultRequeueDelay {
		return fmt.Errorf("mq: %s max_requeue_delay must be greater than or equal to default_requeue_delay", driver)
	}
	if cfg.MaxAttempts > 32767 {
		return fmt.Errorf("mq: %s max_attempts must not exceed notification_event.attempt_count limit 32767", driver)
	}
	return nil
}

// Handler processes one delivery. Returning nil acknowledges it; returning
// any error asks the selected Consumer to use its own requeue mechanism.
type Handler func(ctx context.Context, delivery Delivery) error

// Consumer runs Handler for every delivered message until ctx is canceled
// or Close is called. External MQ consumers can compete across processes;
// MemoryBroker limits concurrency to its in-process goroutine pool.
type Consumer interface {
	// Start begins delivering messages, running up to concurrency Handler
	// invocations at once within this process (concurrency <= 1 means
	// process one message at a time).
	Start(ctx context.Context, concurrency int, handler Handler) error
	Close() error
}

// NewPublisher builds the Publisher selected by cfg.Driver.
func NewPublisher(cfg config.MQConfig) (Publisher, error) {
	switch cfg.Driver {
	case "nsq":
		return newNSQPublisher(cfg.NSQ)
	case "rabbitmq":
		return newRabbitMQPublisher(cfg.RabbitMQ)
	case "memory":
		return nil, fmt.Errorf("mq: memory driver requires one shared MemoryBroker in cmd/server")
	default:
		return nil, fmt.Errorf("mq: unknown driver %q (want \"memory\", \"nsq\" or \"rabbitmq\")", cfg.Driver)
	}
}

// NewConsumer builds the Consumer selected by cfg.Driver.
func NewConsumer(cfg config.MQConfig) (Consumer, error) {
	switch cfg.Driver {
	case "nsq":
		return newNSQConsumer(cfg.NSQ)
	case "rabbitmq":
		return newRabbitMQConsumer(cfg.RabbitMQ)
	case "memory":
		return nil, fmt.Errorf("mq: memory driver requires one shared MemoryBroker in cmd/server")
	default:
		return nil, fmt.Errorf("mq: unknown driver %q (want \"memory\", \"nsq\" or \"rabbitmq\")", cfg.Driver)
	}
}
