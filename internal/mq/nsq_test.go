package mq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	gonsq "github.com/nsqio/go-nsq"
)

type testNSQMessageDelegate struct {
	delay   time.Duration
	backoff bool
}

func (*testNSQMessageDelegate) OnFinish(*gonsq.Message) {}
func (d *testNSQMessageDelegate) OnRequeue(_ *gonsq.Message, delay time.Duration, backoff bool) {
	d.delay = delay
	d.backoff = backoff
}
func (*testNSQMessageDelegate) OnTouch(*gonsq.Message) {}

func TestNSQHandlerReturnsErrorForGoNSQAutomaticRequeue(t *testing.T) {
	id := uuid.New()
	body, err := encode(id)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("provider transient failure")
	var gotDelivery Delivery
	h := &nsqHandlerFunc{
		ctx: context.Background(), defaultRequeueDelay: 3 * time.Second,
		maxRequeueDelay: 10 * time.Second, maxAttempts: 5,
		handler: func(_ context.Context, delivery Delivery) error {
			gotDelivery = delivery
			return want
		},
	}
	message := gonsq.NewMessage(gonsq.MessageID{}, body)
	message.Attempts = 2

	if got := h.HandleMessage(message); !errors.Is(got, want) {
		t.Fatalf("HandleMessage() error = %v, want %v", got, want)
	}
	if gotDelivery.EventID != id || gotDelivery.Attempts != 2 || gotDelivery.MaxAttempts != 5 || gotDelivery.RequeueDelay != 6*time.Second {
		t.Fatalf("delivery = %+v", gotDelivery)
	}
	if message.HasResponded() {
		t.Fatal("handler responded explicitly; go-nsq should issue automatic REQ")
	}
}

func TestNSQHandlerHonorsCircuitDelayWithoutGlobalBackoff(t *testing.T) {
	id := uuid.New()
	body, err := encode(id)
	if err != nil {
		t.Fatal(err)
	}
	h := &nsqHandlerFunc{
		ctx: context.Background(), defaultRequeueDelay: time.Second,
		maxRequeueDelay: time.Minute, maxAttempts: 5,
		handler: func(context.Context, Delivery) error {
			return RequestRequeue(errors.New("circuit open"), 30*time.Second, false)
		},
	}
	delegate := &testNSQMessageDelegate{}
	message := gonsq.NewMessage(gonsq.MessageID{}, body)
	message.Delegate = delegate
	message.Attempts = 2

	if err := h.HandleMessage(message); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	if !message.HasResponded() || delegate.delay != 30*time.Second || delegate.backoff {
		t.Fatalf("requeue response: responded=%t delay=%v backoff=%t", message.HasResponded(), delegate.delay, delegate.backoff)
	}
}

func TestNSQHandlerHonorsProviderDelayWithBackoff(t *testing.T) {
	id := uuid.New()
	body, err := encode(id)
	if err != nil {
		t.Fatal(err)
	}
	h := &nsqHandlerFunc{
		ctx: context.Background(), defaultRequeueDelay: time.Second,
		maxRequeueDelay: time.Minute, maxAttempts: 5,
		handler: func(context.Context, Delivery) error {
			return RequestRequeue(errors.New("provider failure"), 7*time.Second, true)
		},
	}
	delegate := &testNSQMessageDelegate{}
	message := gonsq.NewMessage(gonsq.MessageID{}, body)
	message.Delegate = delegate

	if err := h.HandleMessage(message); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	if !message.HasResponded() || delegate.delay != 7*time.Second || !delegate.backoff {
		t.Fatalf("requeue response: responded=%t delay=%v backoff=%t", message.HasResponded(), delegate.delay, delegate.backoff)
	}
}

func TestLinearRequeueDelayUsesAttemptsAndCap(t *testing.T) {
	if got := linearRequeueDelay(2, 3*time.Second, 10*time.Second); got != 6*time.Second {
		t.Fatalf("delay = %v, want 6s", got)
	}
	if got := linearRequeueDelay(4, 3*time.Second, 10*time.Second); got != 10*time.Second {
		t.Fatalf("capped delay = %v, want 10s", got)
	}
	if got := linearRequeueDelay(0, 3*time.Second, 10*time.Second); got != 3*time.Second {
		t.Fatalf("zero-attempt delay = %v, want 3s", got)
	}
}
