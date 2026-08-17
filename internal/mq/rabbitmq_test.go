package mq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"notification-delivery/internal/config"
)

func TestRabbitDeliveryAttempts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers amqp.Table
		want    uint32
	}{
		{name: "missing header", want: 1},
		{name: "int64 header", headers: amqp.Table{rabbitAttemptsHeader: int64(3)}, want: 3},
		{name: "int32 header", headers: amqp.Table{rabbitAttemptsHeader: int32(4)}, want: 4},
		{name: "invalid zero", headers: amqp.Table{rabbitAttemptsHeader: int64(0)}, want: 1},
		{name: "invalid type", headers: amqp.Table{rabbitAttemptsHeader: "5"}, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rabbitDeliveryAttempts(tc.headers); got != tc.want {
				t.Fatalf("rabbitDeliveryAttempts() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCloneAMQPTableDoesNotMutateOriginal(t *testing.T) {
	original := amqp.Table{"trace_id": "trace-1"}
	cloned := cloneAMQPTable(original)
	cloned[rabbitAttemptsHeader] = int64(2)
	if _, exists := original[rabbitAttemptsHeader]; exists {
		t.Fatal("cloneAMQPTable mutated original headers")
	}
	if cloned["trace_id"] != "trace-1" {
		t.Fatalf("cloned trace_id = %v", cloned["trace_id"])
	}
}

func TestDeliveryRequeueDelayUsesProviderAttempt(t *testing.T) {
	delivery := Delivery{
		RequeueDelay:        99 * time.Second,
		defaultRequeueDelay: 3 * time.Second,
		maxRequeueDelay:     10 * time.Second,
	}
	if got := delivery.RequeueDelayFor(2); got != 6*time.Second {
		t.Fatalf("RequeueDelayFor(2) = %v, want 6s", got)
	}
	if got := delivery.RequeueDelayFor(4); got != 10*time.Second {
		t.Fatalf("RequeueDelayFor(4) = %v, want 10s", got)
	}
}

func TestRequestedRequeueOverridesFallback(t *testing.T) {
	want := errors.New("circuit open")
	err := RequestRequeue(want, 30*time.Second, false)
	delay, backoff, explicit := requestedRequeue(err, time.Second)
	if delay != 30*time.Second || backoff || !explicit || !errors.Is(err, want) {
		t.Fatalf("requested requeue = delay %v backoff %t explicit %t error %v", delay, backoff, explicit, err)
	}
}

func TestRabbitMQDelayedRequeueIntegration(t *testing.T) {
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("TEST_RABBITMQ_URL is not set")
	}
	queue := "notification-delivery-requeue-test-" + uuid.NewString()
	cfg := config.RabbitMQConfig{
		URL: url, Queue: queue, PrefetchCount: 4, ReconnectDelay: 10 * time.Millisecond,
		RequeueConfig: config.RequeueConfig{
			DefaultRequeueDelay: 20 * time.Millisecond,
			MaxRequeueDelay:     40 * time.Millisecond,
			MaxAttempts:         3,
		},
	}
	cleanupRabbitQueue(t, url, queue)

	publisher, err := newRabbitMQPublisher(cfg)
	if err != nil {
		t.Fatalf("newRabbitMQPublisher() error = %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })
	consumer, err := newRabbitMQConsumer(cfg)
	if err != nil {
		t.Fatalf("newRabbitMQConsumer() error = %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	deliveries := make(chan Delivery, 3)
	if err := consumer.Start(ctx, 1, func(_ context.Context, delivery Delivery) error {
		deliveries <- delivery
		if delivery.Attempts < 3 {
			return fmt.Errorf("retry attempt %d", delivery.Attempts)
		}
		return nil
	}); err != nil {
		t.Fatalf("consumer.Start() error = %v", err)
	}
	if _, err := publisher.Publish(ctx, uuid.New()); err != nil {
		t.Fatalf("publisher.Publish() error = %v", err)
	}

	for attempt := uint32(1); attempt <= 3; attempt++ {
		select {
		case delivery := <-deliveries:
			wantDelay := linearRequeueDelay(attempt, cfg.DefaultRequeueDelay, cfg.MaxRequeueDelay)
			if delivery.Attempts != attempt || delivery.MaxAttempts != 3 || delivery.RequeueDelay != wantDelay {
				t.Fatalf("delivery %d metadata = %+v, want delay %v", attempt, delivery, wantDelay)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for RabbitMQ attempt %d", attempt)
		}
	}
}

func cleanupRabbitQueue(t *testing.T, url, queue string) {
	t.Helper()
	t.Cleanup(func() {
		conn, err := amqp.Dial(url)
		if err != nil {
			return
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer ch.Close()
		_, _ = ch.QueueDelete(queue, false, false, false)
	})
}
