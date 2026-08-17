package mq

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"notification-delivery/internal/config"
)

func newTestMemoryBroker(t *testing.T, bufferSize int, requeueDelay time.Duration) *MemoryBroker {
	t.Helper()
	broker, err := NewMemoryBroker(config.MemoryConfig{
		BufferSize: bufferSize,
		RequeueConfig: config.RequeueConfig{
			DefaultRequeueDelay: requeueDelay,
			MaxRequeueDelay:     10 * requeueDelay,
			MaxAttempts:         5,
		},
	}, nil)
	if err != nil {
		t.Fatalf("NewMemoryBroker() error = %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	return broker
}

func TestMemoryBrokerPublishIsNonDurableAndNonBlockingWhenFull(t *testing.T) {
	broker := newTestMemoryBroker(t, 1, time.Millisecond)

	result, err := broker.Publish(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	if result.Durable {
		t.Fatal("memory Publish() unexpectedly returned a durable receipt")
	}

	started := time.Now()
	_, err = broker.Publish(context.Background(), uuid.New())
	if !errors.Is(err, ErrMemoryQueueFull) {
		t.Fatalf("second Publish() error = %v, want ErrMemoryQueueFull", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("full queue blocked for %v", elapsed)
	}
}

func TestMemoryBrokerLimitsHandlerConcurrency(t *testing.T) {
	broker := newTestMemoryBroker(t, 8, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var active atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	if err := broker.Start(ctx, 2, func(context.Context, Delivery) error {
		current := active.Add(1)
		for {
			seen := maximum.Load()
			if current <= seen || maximum.CompareAndSwap(seen, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return nil
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	for range 4 {
		if _, err := broker.Publish(ctx, uuid.New()); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for initial handlers")
		}
	}
	select {
	case <-started:
		t.Fatal("more handlers started than configured concurrency")
	case <-time.After(30 * time.Millisecond):
	}

	close(release)
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for queued handlers")
		}
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrency = %d, want 2", got)
	}
}

func TestMemoryBrokerRequeuesHandlerError(t *testing.T) {
	broker := newTestMemoryBroker(t, 1, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	done := make(chan struct{})
	if err := broker.Start(ctx, 1, func(context.Context, Delivery) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary handler failure")
		}
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := broker.Publish(ctx, uuid.New()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for requeued event")
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestMemoryBrokerHonorsExplicitRequeueDelay(t *testing.T) {
	broker := newTestMemoryBroker(t, 1, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := time.Now()
	done := make(chan time.Duration, 1)
	var calls atomic.Int32
	if err := broker.Start(ctx, 1, func(context.Context, Delivery) error {
		if calls.Add(1) == 1 {
			return RequestRequeue(errors.New("circuit open"), 30*time.Millisecond, false)
		}
		done <- time.Since(started)
		return nil
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := broker.Publish(ctx, uuid.New()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case elapsed := <-done:
		if elapsed < 25*time.Millisecond {
			t.Fatalf("explicit requeue delay was not honored: %v", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for explicit delayed requeue")
	}
}

func TestMemoryBrokerUsesLinearRequeueDelayAndAttempts(t *testing.T) {
	broker := newTestMemoryBroker(t, 1, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	done := make(chan struct{})
	if err := broker.Start(ctx, 1, func(_ context.Context, delivery Delivery) error {
		current := attempts.Add(1)
		if delivery.Attempts != uint32(current) || delivery.MaxAttempts != 5 ||
			delivery.RequeueDelay != time.Duration(current)*5*time.Millisecond {
			t.Errorf("delivery metadata = %+v, handler attempt=%d", delivery, current)
		}
		if current < 3 {
			return errors.New("provider transient failure")
		}
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := broker.Publish(ctx, uuid.New()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("requested requeue delay was not used")
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

func TestMemoryBrokerDelayedRequeueDoesNotOccupyConsumerSlot(t *testing.T) {
	broker := newTestMemoryBroker(t, 2, 500*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	retryID := uuid.New()
	otherID := uuid.New()
	processedOther := make(chan struct{})
	if err := broker.Start(ctx, 1, func(_ context.Context, delivery Delivery) error {
		id := delivery.EventID
		if id == retryID {
			return errors.New("retry later")
		}
		if id == otherID {
			close(processedOther)
		}
		return nil
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := broker.Publish(ctx, retryID); err != nil {
		t.Fatalf("Publish(retryID) error = %v", err)
	}
	if _, err := broker.Publish(ctx, otherID); err != nil {
		t.Fatalf("Publish(otherID) error = %v", err)
	}

	select {
	case <-processedOther:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("delayed requeue occupied the only consumer slot")
	}
}

func TestMemoryBrokerEmitsDebugLifecycleLogs(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	broker, err := NewMemoryBroker(config.MemoryConfig{
		BufferSize: 1,
		RequeueConfig: config.RequeueConfig{
			DefaultRequeueDelay: time.Millisecond,
			MaxRequeueDelay:     time.Second,
			MaxAttempts:         5,
		},
	}, logger)
	if err != nil {
		t.Fatalf("NewMemoryBroker() error = %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	if err := broker.Start(ctx, 1, func(context.Context, Delivery) error {
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := broker.Publish(ctx, uuid.New()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for handler")
	}
	if err := broker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	for _, message := range []string{
		"memory mq consumer started",
		"memory mq event published",
		"memory mq event dequeued",
	} {
		if !strings.Contains(output.String(), message) {
			t.Errorf("debug output does not contain %q: %s", message, output.String())
		}
	}
}

func TestMemoryBrokerConcurrentPublishAndClose(t *testing.T) {
	broker := newTestMemoryBroker(t, 32, time.Millisecond)
	ctx := context.Background()
	if err := broker.Start(ctx, 4, func(context.Context, Delivery) error { return nil }); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_, _ = broker.Publish(ctx, uuid.New())
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = broker.Close()
		}()
	}
	wg.Wait()

	if _, err := broker.Publish(ctx, uuid.New()); !errors.Is(err, ErrMemoryClosed) {
		t.Fatalf("Publish() after Close error = %v, want ErrMemoryClosed", err)
	}
}

func TestNewMemoryBrokerValidatesConfig(t *testing.T) {
	for _, cfg := range []config.MemoryConfig{
		{BufferSize: 0, RequeueConfig: config.RequeueConfig{DefaultRequeueDelay: time.Second, MaxRequeueDelay: time.Minute}},
		{BufferSize: 1, RequeueConfig: config.RequeueConfig{DefaultRequeueDelay: 0, MaxRequeueDelay: time.Minute}},
		{BufferSize: 1, RequeueConfig: config.RequeueConfig{DefaultRequeueDelay: time.Second, MaxRequeueDelay: time.Millisecond}},
		{BufferSize: 1, RequeueConfig: config.RequeueConfig{DefaultRequeueDelay: time.Second, MaxRequeueDelay: time.Minute, MaxAttempts: 32768}},
	} {
		if _, err := NewMemoryBroker(cfg, nil); err == nil {
			t.Fatalf("NewMemoryBroker(%+v) unexpectedly succeeded", cfg)
		}
	}
}
