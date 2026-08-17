package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"notification-delivery/internal/domain"
	"notification-delivery/internal/mq"
	"notification-delivery/internal/provider"
)

type retryTestRepo struct {
	leaseUntil        time.Time
	scheduledResult   json.RawMessage
	circuitLeaseUntil time.Time
	circuitResult     json.RawMessage
	failedResult      json.RawMessage
	failedWithoutCall json.RawMessage
	requestingCalls   int
}

func (*retryTestRepo) Claim(context.Context, uuid.UUID, uuid.UUID, time.Duration) (*domain.Event, error) {
	panic("not used")
}
func (r *retryTestRepo) MarkRequesting(context.Context, uuid.UUID, uuid.UUID) error {
	r.requestingCalls++
	return nil
}
func (*retryTestRepo) RenewLease(context.Context, uuid.UUID, uuid.UUID, time.Duration) error {
	return nil
}
func (r *retryTestRepo) MarkWaitingForRequeue(_ context.Context, _, _ uuid.UUID, leaseUntil time.Time, result, _ json.RawMessage) error {
	r.leaseUntil = leaseUntil
	r.scheduledResult = result
	return nil
}
func (r *retryTestRepo) MarkCircuitOpenWaitingForRequeue(_ context.Context, _, _ uuid.UUID, leaseUntil time.Time, result json.RawMessage) error {
	r.circuitLeaseUntil = leaseUntil
	r.circuitResult = result
	return nil
}
func (*retryTestRepo) MarkSucceeded(context.Context, uuid.UUID, uuid.UUID, json.RawMessage, json.RawMessage) error {
	panic("not used")
}
func (r *retryTestRepo) MarkFailed(_ context.Context, _, _ uuid.UUID, result, _ json.RawMessage) error {
	r.failedResult = result
	return nil
}
func (r *retryTestRepo) MarkFailedWithoutProviderAttempt(_ context.Context, _, _ uuid.UUID, result json.RawMessage) error {
	r.failedWithoutCall = result
	return nil
}

type retryTestRegistry struct {
	resolved provider.ResolvedAction
}

func (r retryTestRegistry) Lookup(_, _ string) (provider.ResolvedAction, bool) {
	return r.resolved, true
}

type retryTestAdapter struct {
	result *provider.Result
	err    error
	calls  *atomic.Int32
}

func (retryTestAdapter) ProviderCode() string { return "test" }
func (retryTestAdapter) Config(yaml.Node) (provider.Config, error) {
	panic("not used")
}
func (retryTestAdapter) Validate(string, json.RawMessage) error { return nil }
func (a retryTestAdapter) SendActionRequest(context.Context, provider.ActionContext, string, json.RawMessage) (*provider.Result, error) {
	if a.calls != nil {
		a.calls.Add(1)
	}
	return a.result, a.err
}

func newRetryTestProcessor(repo *retryTestRepo, attempt int16) (*Processor, *domain.Event, uuid.UUID) {
	result := &provider.Result{
		HTTPStatus: 429,
		ErrorClass: "VENDOR_TRANSIENT_ERROR",
		Message:    "rate limited",
	}
	limits := NewLimiters()
	limits.Register("test/send", 1000, 1)
	p := &Processor{
		Repo: repo,
		Registry: retryTestRegistry{resolved: provider.ResolvedAction{
			Adapter: retryTestAdapter{result: result},
			Context: provider.ActionContext{ProviderCode: "test", ProviderAction: "send"},
		}},
		Limiters:      limits,
		LeaseDuration: time.Minute,
	}
	ev := &domain.Event{
		ID: uuid.New(), ProviderCode: "test", ProviderAction: "send",
		AttemptCount: attempt, Payload: json.RawMessage(`{"value":1}`),
	}
	return p, ev, uuid.New()
}

func TestProcessorReleasesEveryProviderFailureForMQRequeue(t *testing.T) {
	repo := &retryTestRepo{}
	p, ev, leaseToken := newRetryTestProcessor(repo, 1)
	started := time.Now()

	err := p.process(context.Background(), ev, leaseToken, mq.Delivery{
		Attempts: 1, MaxAttempts: 3, RequeueDelay: 2 * time.Second,
	})
	if err == nil {
		t.Fatal("process() error = nil, want MQ requeue signal")
	}
	if repo.leaseUntil.Before(started.Add(61900*time.Millisecond)) || repo.leaseUntil.After(time.Now().Add(62100*time.Millisecond)) {
		t.Fatalf("leaseUntil = %v, want requeue delay plus recovery lease", repo.leaseUntil)
	}
	var result domain.LastResultFinished
	if err := json.Unmarshal(repo.scheduledResult, &result); err != nil {
		t.Fatalf("unmarshal scheduled result: %v", err)
	}
	if result.Outcome != domain.OutcomeRequeueRequested || !result.Retryable {
		t.Fatalf("scheduled result = %+v", result)
	}
}

func TestProcessorReleasesAdapterErrorForMQRequeue(t *testing.T) {
	repo := &retryTestRepo{}
	p, ev, leaseToken := newRetryTestProcessor(repo, 1)
	p.Registry = retryTestRegistry{resolved: provider.ResolvedAction{
		Adapter: retryTestAdapter{err: errors.New("build request failed")},
		Context: provider.ActionContext{ProviderCode: "test", ProviderAction: "send"},
	}}

	err := p.process(context.Background(), ev, leaseToken, mq.Delivery{
		Attempts: 1, MaxAttempts: 3, RequeueDelay: time.Second,
	})
	if err == nil {
		t.Fatal("process() error = nil, want MQ requeue signal")
	}
	var result domain.LastResultFinished
	if err := json.Unmarshal(repo.scheduledResult, &result); err != nil {
		t.Fatalf("unmarshal scheduled result: %v", err)
	}
	if result.Outcome != domain.OutcomeRequeueRequested || result.ErrorClass != "ADAPTER_ERROR" {
		t.Fatalf("scheduled result = %+v", result)
	}
}

func TestProcessorMarksFailedOnPersistedProviderAttemptLimit(t *testing.T) {
	repo := &retryTestRepo{}
	p, ev, leaseToken := newRetryTestProcessor(repo, 3)
	calls := &atomic.Int32{}
	p.Registry = retryTestRegistry{resolved: provider.ResolvedAction{
		Adapter: retryTestAdapter{calls: calls, result: &provider.Result{
			HTTPStatus: 429,
			ErrorClass: "VENDOR_TRANSIENT_ERROR",
			Message:    "rate limited",
		}},
		Context: provider.ActionContext{ProviderCode: "test", ProviderAction: "send"},
	}}

	err := p.process(context.Background(), ev, leaseToken, mq.Delivery{
		// Memory attempts can restart from one after process recovery; the
		// persisted attempt_count must still enforce the configured maximum.
		Attempts: 1, MaxAttempts: 3, RequeueDelay: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("process() error = %v, want ACK after terminal database update", err)
	}
	if !repo.leaseUntil.IsZero() {
		t.Fatalf("final attempt scheduled another requeue at %v", repo.leaseUntil)
	}
	if calls.Load() != 1 {
		t.Fatalf("provider calls = %d, want final allowed call", calls.Load())
	}
	var result domain.LastResultFinished
	if err := json.Unmarshal(repo.failedResult, &result); err != nil {
		t.Fatalf("unmarshal failed result: %v", err)
	}
	if result.Outcome != domain.OutcomeFailed || result.ErrorClass != "MAX_PROVIDER_ATTEMPTS_EXHAUSTED" || result.AttemptNumber != 3 {
		t.Fatalf("failed result = %+v", result)
	}
}

func TestProcessorSkipsProviderWhenCrashRecoveryExceedsAttemptLimit(t *testing.T) {
	repo := &retryTestRepo{}
	p, ev, leaseToken := newRetryTestProcessor(repo, 4)
	calls := &atomic.Int32{}
	p.Registry = retryTestRegistry{resolved: provider.ResolvedAction{
		Adapter: retryTestAdapter{calls: calls, result: &provider.Result{Success: true}},
		Context: provider.ActionContext{ProviderCode: "test", ProviderAction: "send"},
	}}
	// Neither dependency may be touched when the pre-provider guard works.
	p.Limiters = nil
	p.Breakers = nil

	err := p.process(context.Background(), ev, leaseToken, mq.Delivery{
		Attempts: 1, MaxAttempts: 3, RequeueDelay: time.Second,
	})
	if err != nil {
		t.Fatalf("process() error = %v, want ACK after terminal database update", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("provider calls = %d, want 0", calls.Load())
	}
	if repo.requestingCalls != 0 {
		t.Fatalf("MarkRequesting calls = %d, want 0", repo.requestingCalls)
	}
	if len(repo.failedResult) != 0 {
		t.Fatalf("used post-provider failure transition: %s", repo.failedResult)
	}
	var result domain.LastResultFinished
	if err := json.Unmarshal(repo.failedWithoutCall, &result); err != nil {
		t.Fatalf("unmarshal failed-without-call result: %v", err)
	}
	if result.Outcome != domain.OutcomeFailed || result.ErrorClass != "MAX_PROVIDER_ATTEMPTS_EXHAUSTED" || result.AttemptNumber != 3 {
		t.Fatalf("failed result = %+v", result)
	}
}

func TestProcessorUsesDatabaseProviderAttemptsNotBrokerAttempts(t *testing.T) {
	repo := &retryTestRepo{}
	p, ev, leaseToken := newRetryTestProcessor(repo, 1)

	err := p.process(context.Background(), ev, leaseToken, mq.Delivery{
		Attempts: 99, MaxAttempts: 3, RequeueDelay: time.Second,
	})
	if err == nil {
		t.Fatal("process() error = nil, want requeue")
	}
	if len(repo.failedResult) != 0 {
		t.Fatalf("broker attempts incorrectly exhausted provider budget: %s", repo.failedResult)
	}
}

func TestProcessorOpenCircuitDefersWithoutProviderAttempt(t *testing.T) {
	repo := &retryTestRepo{}
	p, first, firstLease := newRetryTestProcessor(repo, 1)
	calls := &atomic.Int32{}
	p.Registry = retryTestRegistry{resolved: provider.ResolvedAction{
		Adapter: retryTestAdapter{calls: calls, result: &provider.Result{
			AvailabilityFailure: true,
			HTTPStatus:          503,
			ErrorClass:          "PROVIDER_UNAVAILABLE",
			Message:             "unavailable",
		}},
		Context: provider.ActionContext{ProviderCode: "test", ProviderAction: "send"},
	}}
	p.Breakers = NewBreakers(nil)
	p.Breakers.Register("test/send", &provider.CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     30 * time.Second,
	})
	delivery := mq.Delivery{Attempts: 1, MaxAttempts: 3, RequeueDelay: time.Second}
	if err := p.process(context.Background(), first, firstLease, delivery); err == nil {
		t.Fatal("first provider failure did not request requeue")
	}
	if repo.requestingCalls != 1 {
		t.Fatalf("MarkRequesting calls after provider attempt = %d, want 1", repo.requestingCalls)
	}

	second := *first
	second.AttemptCount = 2 // Claim incremented before the open-circuit check.
	// A nil limiter would panic if the open-circuit path tried to acquire it.
	p.Limiters = nil
	err := p.process(context.Background(), &second, uuid.New(), delivery)
	var requeue *mq.RequeueError
	if !errors.As(err, &requeue) {
		t.Fatalf("open circuit error = %v, want RequeueError", err)
	}
	if requeue.Backoff || requeue.Delay < 29*time.Second || requeue.Delay > 31*time.Second {
		t.Fatalf("open circuit requeue = %+v", requeue)
	}
	if calls.Load() != 1 {
		t.Fatalf("provider calls = %d, want 1", calls.Load())
	}
	if repo.requestingCalls != 1 {
		t.Fatalf("open circuit called MarkRequesting; total calls = %d", repo.requestingCalls)
	}
	if len(repo.failedResult) != 0 {
		t.Fatalf("circuit deferral exhausted provider attempts: %s", repo.failedResult)
	}
	var result domain.LastResultFinished
	if err := json.Unmarshal(repo.circuitResult, &result); err != nil {
		t.Fatalf("unmarshal circuit result: %v", err)
	}
	if result.ErrorClass != "CIRCUIT_OPEN" || result.AttemptNumber != 1 || result.Outcome != domain.OutcomeRequeueRequested {
		t.Fatalf("circuit result = %+v", result)
	}
}
