package worker

import (
	"log/slog"
	"sync"
	"time"

	"notification-delivery/internal/provider"
)

type breakerState string

const (
	breakerClosed   breakerState = "CLOSED"
	breakerOpen     breakerState = "OPEN"
	breakerHalfOpen breakerState = "HALF_OPEN"
)

// BreakerObservation describes only provider availability. It is deliberately
// independent of whether the event itself is retryable.
type BreakerObservation uint8

const (
	BreakerIgnore BreakerObservation = iota
	BreakerReachable
	BreakerUnavailable
)

type BreakerPermit struct {
	breaker    *actionBreaker
	generation uint64
	halfOpen   bool
}

type BreakerDecision struct {
	Allowed    bool
	Permit     BreakerPermit
	RetryAfter time.Duration
}

type actionBreaker struct {
	mu sync.Mutex

	key              string
	failureThreshold uint32
	openDuration     time.Duration
	now              func() time.Time
	logger           *slog.Logger

	state              breakerState
	consecutiveFailure uint32
	generation         uint64
	openUntil          time.Time
	halfOpenInFlight   bool
}

// Breakers owns one process-local breaker per configured provider action.
// Separate worker replicas intentionally keep independent state.
type Breakers struct {
	mu     sync.RWMutex
	m      map[string]*actionBreaker
	now    func() time.Time
	logger *slog.Logger
}

func NewBreakers(logger *slog.Logger) *Breakers {
	return newBreakersWithClock(logger, time.Now)
}

func newBreakersWithClock(logger *slog.Logger, now func() time.Time) *Breakers {
	return &Breakers{m: make(map[string]*actionBreaker), now: now, logger: logger}
}

func (b *Breakers) Register(key string, cfg *provider.CircuitBreakerConfig) {
	if b == nil || cfg == nil {
		return
	}
	ab := &actionBreaker{
		key:              key,
		failureThreshold: cfg.FailureThreshold,
		openDuration:     cfg.OpenDuration,
		now:              b.now,
		logger:           b.logger,
		state:            breakerClosed,
	}
	b.mu.Lock()
	b.m[key] = ab
	b.mu.Unlock()
}

func (b *Breakers) Allow(key string) BreakerDecision {
	if b == nil {
		return BreakerDecision{Allowed: true}
	}
	b.mu.RLock()
	ab := b.m[key]
	b.mu.RUnlock()
	if ab == nil {
		return BreakerDecision{Allowed: true}
	}
	return ab.allow()
}

func (b *Breakers) Observe(permit BreakerPermit, observation BreakerObservation) {
	if permit.breaker != nil {
		permit.breaker.observe(permit, observation)
	}
}

func (b *Breakers) Abort(permit BreakerPermit) {
	if permit.breaker != nil {
		permit.breaker.abort(permit)
	}
}

func (b *actionBreaker) allow() BreakerDecision {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	switch b.state {
	case breakerOpen:
		if now.Before(b.openUntil) {
			return BreakerDecision{RetryAfter: b.openUntil.Sub(now)}
		}
		b.transitionLocked(breakerHalfOpen)
		b.halfOpenInFlight = true
		return BreakerDecision{
			Allowed: true,
			Permit:  BreakerPermit{breaker: b, generation: b.generation, halfOpen: true},
		}
	case breakerHalfOpen:
		if b.halfOpenInFlight {
			return BreakerDecision{}
		}
		b.halfOpenInFlight = true
		return BreakerDecision{
			Allowed: true,
			Permit:  BreakerPermit{breaker: b, generation: b.generation, halfOpen: true},
		}
	default:
		return BreakerDecision{
			Allowed: true,
			Permit:  BreakerPermit{breaker: b, generation: b.generation},
		}
	}
}

func (b *actionBreaker) observe(permit BreakerPermit, observation BreakerObservation) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if permit.generation != b.generation {
		return
	}

	if permit.halfOpen {
		b.halfOpenInFlight = false
	}
	switch observation {
	case BreakerIgnore:
		return
	case BreakerReachable:
		b.consecutiveFailure = 0
		if b.state == breakerHalfOpen {
			b.generation++
			b.openUntil = time.Time{}
			b.transitionLocked(breakerClosed)
		}
	case BreakerUnavailable:
		if b.state == breakerHalfOpen {
			b.openLocked()
			return
		}
		if b.state != breakerClosed {
			return
		}
		b.consecutiveFailure++
		if b.consecutiveFailure >= b.failureThreshold {
			b.openLocked()
		}
	}
}

func (b *actionBreaker) abort(permit BreakerPermit) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if permit.generation == b.generation && permit.halfOpen {
		b.halfOpenInFlight = false
	}
}

func (b *actionBreaker) openLocked() {
	b.generation++
	b.openUntil = b.now().Add(b.openDuration)
	b.halfOpenInFlight = false
	b.transitionLocked(breakerOpen)
}

func (b *actionBreaker) transitionLocked(next breakerState) {
	previous := b.state
	if previous == next {
		return
	}
	b.state = next
	if b.logger == nil {
		return
	}
	attrs := []any{
		"action", b.key,
		"previous_state", previous,
		"state", next,
		"consecutive_failures", b.consecutiveFailure,
		"open_until", b.openUntil.UTC(),
	}
	b.logger.Warn("provider circuit breaker state changed", attrs...)
}
