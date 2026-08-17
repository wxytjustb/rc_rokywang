package worker

import (
	"testing"
	"time"

	"notification-delivery/internal/provider"
)

func TestBreakerClosedOpenHalfOpenLifecycle(t *testing.T) {
	now := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	breakers := newBreakersWithClock(nil, func() time.Time { return now })
	breakers.Register("test/send", &provider.CircuitBreakerConfig{
		FailureThreshold: 2,
		OpenDuration:     30 * time.Second,
	})

	first := breakers.Allow("test/send")
	if !first.Allowed {
		t.Fatal("first request was not allowed")
	}
	breakers.Observe(first.Permit, BreakerUnavailable)
	second := breakers.Allow("test/send")
	breakers.Observe(second.Permit, BreakerUnavailable)

	open := breakers.Allow("test/send")
	if open.Allowed || open.RetryAfter != 30*time.Second {
		t.Fatalf("open decision = %+v", open)
	}

	now = now.Add(30 * time.Second)
	probe := breakers.Allow("test/send")
	if !probe.Allowed || !probe.Permit.halfOpen {
		t.Fatalf("half-open probe = %+v", probe)
	}
	if concurrent := breakers.Allow("test/send"); concurrent.Allowed {
		t.Fatal("second half-open probe was allowed")
	}
	breakers.Observe(probe.Permit, BreakerReachable)
	if closed := breakers.Allow("test/send"); !closed.Allowed || closed.Permit.halfOpen {
		t.Fatalf("closed decision = %+v", closed)
	}
}

func TestBreakerFailedProbeReopensAndIgnoresStalePermit(t *testing.T) {
	now := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	breakers := newBreakersWithClock(nil, func() time.Time { return now })
	breakers.Register("test/send", &provider.CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     time.Minute,
	})

	stale := breakers.Allow("test/send")
	breakers.Observe(stale.Permit, BreakerUnavailable)
	breakers.Observe(stale.Permit, BreakerReachable)
	if decision := breakers.Allow("test/send"); decision.Allowed {
		t.Fatal("stale success closed the open circuit")
	}

	now = now.Add(time.Minute)
	probe := breakers.Allow("test/send")
	breakers.Observe(probe.Permit, BreakerUnavailable)
	if decision := breakers.Allow("test/send"); decision.Allowed || decision.RetryAfter != time.Minute {
		t.Fatalf("failed probe did not reopen circuit: %+v", decision)
	}
}

func TestBreakerIgnoreAndAbortReleaseHalfOpenProbe(t *testing.T) {
	now := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	breakers := newBreakersWithClock(nil, func() time.Time { return now })
	breakers.Register("test/send", &provider.CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     time.Second,
	})
	permit := breakers.Allow("test/send")
	breakers.Observe(permit.Permit, BreakerUnavailable)
	now = now.Add(time.Second)

	probe := breakers.Allow("test/send")
	breakers.Abort(probe.Permit)
	probe = breakers.Allow("test/send")
	breakers.Observe(probe.Permit, BreakerIgnore)
	if next := breakers.Allow("test/send"); !next.Allowed {
		t.Fatal("ignored half-open probe did not release its slot")
	}
}

func TestBreakerDisabledActionAlwaysAllows(t *testing.T) {
	breakers := NewBreakers(nil)
	decision := breakers.Allow("not-configured/send")
	if !decision.Allowed {
		t.Fatal("unconfigured action was blocked")
	}
	breakers.Observe(decision.Permit, BreakerUnavailable)
}
