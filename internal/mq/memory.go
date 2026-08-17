package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"notification-delivery/internal/config"
)

var (
	ErrMemoryQueueFull = errors.New("mq: memory queue is full")
	ErrMemoryClosed    = errors.New("mq: memory broker is closed")
)

// MemoryBroker is a non-durable, in-process delivery backend. The same
// instance must be shared by the API publisher and the embedded worker
// consumer. PostgreSQL remains the source of truth and recovers any wake-up
// that cannot be retained by this channel.
type MemoryBroker struct {
	jobs                chan memoryMessage
	defaultRequeueDelay time.Duration
	maxRequeueDelay     time.Duration
	maxAttempts         uint32
	done                chan struct{}
	logger              *slog.Logger

	mu        sync.RWMutex
	started   bool
	closed    bool
	closeOnce sync.Once
	workers   sync.WaitGroup
	requeues  sync.WaitGroup
}

type memoryMessage struct {
	eventID  uuid.UUID
	attempts uint32
}

func NewMemoryBroker(cfg config.MemoryConfig, logger *slog.Logger) (*MemoryBroker, error) {
	if cfg.BufferSize <= 0 {
		return nil, fmt.Errorf("mq: memory buffer_size must be greater than zero")
	}
	if err := validateRequeueConfig("memory", cfg.RequeueConfig); err != nil {
		return nil, err
	}
	return &MemoryBroker{
		jobs:                make(chan memoryMessage, cfg.BufferSize),
		defaultRequeueDelay: cfg.DefaultRequeueDelay,
		maxRequeueDelay:     cfg.MaxRequeueDelay,
		maxAttempts:         cfg.MaxAttempts,
		done:                make(chan struct{}),
		logger:              logger,
	}, nil
}

// Publish performs a non-blocking channel send. A full channel is reported
// immediately so the caller can rely on the database compensator instead of
// adding API latency. The receipt is deliberately non-durable.
func (b *MemoryBroker) Publish(ctx context.Context, eventID uuid.UUID) (PublishResult, error) {
	return b.publishMessage(ctx, memoryMessage{eventID: eventID, attempts: 1})
}

func (b *MemoryBroker) publishMessage(ctx context.Context, message memoryMessage) (PublishResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return PublishResult{}, ErrMemoryClosed
	}
	select {
	case <-ctx.Done():
		return PublishResult{}, ctx.Err()
	case b.jobs <- message:
		b.debug("memory mq event published", "event_id", message.eventID, "attempts", message.attempts,
			"queue_depth", len(b.jobs), "queue_capacity", cap(b.jobs))
		return PublishResult{Durable: false}, nil
	default:
		return PublishResult{}, ErrMemoryQueueFull
	}
}

// Start launches a fixed-size goroutine pool. It may be called only once.
func (b *MemoryBroker) Start(ctx context.Context, concurrency int, handler Handler) error {
	if handler == nil {
		return fmt.Errorf("mq: memory handler must not be nil")
	}
	if concurrency < 1 {
		concurrency = 1
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrMemoryClosed
	}
	if b.started {
		b.mu.Unlock()
		return fmt.Errorf("mq: memory consumer already started")
	}
	b.started = true
	b.workers.Add(concurrency)
	for range concurrency {
		go b.consume(ctx, handler)
	}
	b.mu.Unlock()
	b.debug("memory mq consumer started", "concurrency", concurrency, "queue_capacity", cap(b.jobs))
	return nil
}

func (b *MemoryBroker) consume(ctx context.Context, handler Handler) {
	defer b.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.done:
			return
		case message := <-b.jobs:
			delay := linearRequeueDelay(message.attempts, b.defaultRequeueDelay, b.maxRequeueDelay)
			b.debug("memory mq event dequeued", "event_id", message.eventID, "attempts", message.attempts, "queue_depth", len(b.jobs))
			if err := handler(ctx, Delivery{
				EventID: message.eventID, Attempts: message.attempts,
				MaxAttempts: b.maxAttempts, RequeueDelay: delay,
				defaultRequeueDelay: b.defaultRequeueDelay,
				maxRequeueDelay:     b.maxRequeueDelay,
			}); err != nil {
				requestedDelay, _, _ := requestedRequeue(err, delay)
				b.warn("memory mq handler requested requeue", "event_id", message.eventID,
					"attempts", message.attempts, "delay", requestedDelay, "error", err)
				b.scheduleRequeue(ctx, message, requestedDelay, err)
			} else {
				b.debug("memory mq handler completed; event acknowledged", "event_id", message.eventID, "attempts", message.attempts)
			}
		}
	}
}

func (b *MemoryBroker) scheduleRequeue(ctx context.Context, message memoryMessage, delay time.Duration, handlerErr error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return
	}
	b.requeues.Add(1)
	b.mu.RUnlock()
	go func() {
		defer b.requeues.Done()
		b.requeueAfter(ctx, message, delay, handlerErr)
	}()
}

func (b *MemoryBroker) requeueAfter(ctx context.Context, message memoryMessage, delay time.Duration, handlerErr error) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-b.done:
		return
	case <-timer.C:
	}

	nextAttempts := message.attempts + 1
	if nextAttempts == 0 {
		nextAttempts = message.attempts
	}
	message.attempts = nextAttempts
	if _, err := b.publishMessage(ctx, message); err != nil {
		b.warn("memory mq requeue dropped; relying on database compensator",
			"event_id", message.eventID, "attempts", message.attempts,
			"handler_error", handlerErr, "requeue_error", err)
		return
	}
	b.debug("memory mq event requeued", "event_id", message.eventID, "attempts", message.attempts, "delay", delay)
}

// Ready reports whether the consumer pool is available to drain the channel.
func (b *MemoryBroker) Ready() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrMemoryClosed
	}
	if !b.started {
		return fmt.Errorf("mq: memory consumer has not started")
	}
	return nil
}

// Close stops intake and waits for all consumer goroutines. The jobs channel
// is intentionally never closed, which makes concurrent Publish/Close safe.
func (b *MemoryBroker) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		close(b.done)
		b.mu.Unlock()
		b.workers.Wait()
		b.requeues.Wait()
	})
	return nil
}

func (b *MemoryBroker) debug(message string, args ...any) {
	if b.logger != nil {
		b.logger.Debug(message, args...)
	}
}

func (b *MemoryBroker) warn(message string, args ...any) {
	if b.logger != nil {
		b.logger.Warn(message, args...)
		return
	}
	slog.Warn(message, args...)
}
