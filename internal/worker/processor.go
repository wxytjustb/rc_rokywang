// Package worker implements the Delivery Worker and its embedded
// compensator (DESIGN.md §2.2, §6, §8, §9).
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"notification-delivery/internal/domain"
	"notification-delivery/internal/mq"
	"notification-delivery/internal/provider"
	"notification-delivery/internal/store"
)

// Processor takes one event_id from the broker through claim, provider
// call, and final state transition. It has no knowledge of which MQ
// backend delivered the id, nor of how an Adapter talks to its vendor —
// building the request, sending it, and confirming success are all
// the Adapter's job (internal/provider.Adapter.SendActionRequest).
//
// Every provider failure remains PROCESSING in a lease-free requeue substate
// before broker redelivery. Only explicit success is acknowledged; the MQ
// maximum attempt count decides when a failure becomes terminal.
type eventRepository interface {
	Claim(context.Context, uuid.UUID, uuid.UUID, time.Duration) (*domain.Event, error)
	MarkRequesting(context.Context, uuid.UUID, uuid.UUID) error
	RenewLease(context.Context, uuid.UUID, uuid.UUID, time.Duration) error
	MarkWaitingForRequeue(context.Context, uuid.UUID, uuid.UUID, time.Time, json.RawMessage, json.RawMessage) error
	MarkCircuitOpenWaitingForRequeue(context.Context, uuid.UUID, uuid.UUID, time.Time, json.RawMessage) error
	MarkSucceeded(context.Context, uuid.UUID, uuid.UUID, json.RawMessage, json.RawMessage) error
	MarkFailed(context.Context, uuid.UUID, uuid.UUID, json.RawMessage, json.RawMessage) error
	MarkFailedWithoutProviderAttempt(context.Context, uuid.UUID, uuid.UUID, json.RawMessage) error
}

type actionRegistry interface {
	Lookup(providerCode, providerAction string) (provider.ResolvedAction, bool)
}

type Processor struct {
	Repo          eventRepository
	Registry      actionRegistry
	Limiters      *Limiters
	Breakers      *Breakers
	LeaseDuration time.Duration
	Logger        *slog.Logger
}

// HandleDelivery is the mq.Handler used by every Consumer. Persisted provider
// attempts drive retry limits and ordinary delay; the MQ executes the returned
// typed requeue directive and keeps broker attempts only for observability.
func (p *Processor) HandleDelivery(ctx context.Context, delivery mq.Delivery) error {
	id := delivery.EventID
	p.debug("worker event received", "event_id", id)
	leaseToken := uuid.New()
	ev, err := p.Repo.Claim(ctx, id, leaseToken, p.LeaseDuration)
	if err != nil {
		if errors.Is(err, store.ErrNotClaimed) {
			// Already claimed by another worker, already finished, or not
			// yet due — per DESIGN.md §6.1 the MQ message is simply ACKed.
			p.debug("worker event not claimable; acknowledging wake-up", "event_id", id)
			return nil
		}
		return err
	}
	p.debug("worker event claimed",
		"event_id", ev.ID,
		"attempt_number", ev.AttemptCount,
		"lease_until", ev.LeaseUntil)
	return p.process(ctx, ev, leaseToken, delivery)
}

func (p *Processor) process(ctx context.Context, ev *domain.Event, leaseToken uuid.UUID, delivery mq.Delivery) error {
	// Claim increments attempt_count before processing. After crash recovery,
	// the previous provider call may already have consumed the final allowed
	// attempt, so a new claim can make the count max_attempts+1. Stop before
	// Registry, breaker, limiter, REQUESTING, and the provider call; the store
	// rolls back this claim-only increment when writing the terminal result.
	if delivery.MaxAttempts > 0 && ev.AttemptCount > 0 && uint32(ev.AttemptCount) > delivery.MaxAttempts {
		return p.finishExhaustedBeforeProvider(ctx, ev, leaseToken, delivery.MaxAttempts)
	}

	resolved, ok := p.Registry.Lookup(ev.ProviderCode, ev.ProviderAction)
	if !ok {
		return p.retryOrFail(ctx, ev, leaseToken, delivery, &provider.Result{
			ErrorClass: "UNSUPPORTED_PROVIDER_ACTION",
			Message:    "provider action is no longer configured",
		}, 0)
	}

	actionKey := ev.ProviderCode + "/" + ev.ProviderAction
	decision := p.Breakers.Allow(actionKey)
	if !decision.Allowed {
		return p.deferForOpenCircuit(ctx, ev, leaseToken, delivery, decision.RetryAfter)
	}
	permit := decision.Permit
	release, err := p.Limiters.Acquire(ctx, actionKey)
	if err != nil {
		p.Breakers.Abort(permit)
		// Context canceled (shutdown) while waiting for a slot: leave the row
		// PROCESSING. Its lease expires and the compensator republishes it
		// through the same requeue state used by provider failures.
		return err
	}
	defer release()

	if err := p.Repo.MarkRequesting(ctx, ev.ID, leaseToken); err != nil {
		p.Breakers.Abort(permit)
		if errors.Is(err, store.ErrNotClaimed) {
			return nil
		}
		return err
	}

	// Renew to the full lease duration right before the outbound call so a
	// slow claim phase never eats into the time budget the call itself
	// needs (DESIGN.md §9's "long-running workers must renew").
	if err := p.Repo.RenewLease(ctx, ev.ID, leaseToken, p.LeaseDuration); err != nil {
		p.Breakers.Abort(permit)
		if errors.Is(err, store.ErrNotClaimed) {
			return nil
		}
		return err
	}

	ac := resolved.Context
	ac.SourceRequestID = ev.SourceRequestID

	p.debug("provider request starting",
		"event_id", ev.ID,
		"provider_code", ev.ProviderCode,
		"provider_action", ev.ProviderAction,
		"attempt_number", ev.AttemptCount,
		"timeout_ms", ac.TimeoutMs)
	start := time.Now()
	result, sendErr := resolved.Adapter.SendActionRequest(ctx, ac, ev.ProviderAction, ev.Payload)
	latency := time.Since(start)

	observation := BreakerIgnore
	if sendErr != nil {
		result = &provider.Result{ErrorClass: "ADAPTER_ERROR", Message: sendErr.Error()}
	} else if result == nil {
		result = &provider.Result{
			ErrorClass: "ADAPTER_CONTRACT_VIOLATION",
			Message:    "adapter returned a nil result",
		}
	} else if result.AvailabilityFailure {
		observation = BreakerUnavailable
	} else {
		// Success and explicit non-availability provider responses both prove
		// that the action endpoint is reachable.
		observation = BreakerReachable
	}
	p.Breakers.Observe(permit, observation)
	p.logProviderRequest(ctx, ev, ac, result, latency)

	if result.Success {
		lastResult, _ := json.Marshal(domain.LastResultFinished{
			Outcome: domain.OutcomeSucceeded, HTTPStatus: result.HTTPStatus,
			LatencyMs: latency.Milliseconds(), AttemptNumber: ev.AttemptCount, FinishedAt: time.Now().UTC(),
		})
		return p.settle(ctx, p.Repo.MarkSucceeded(ctx, ev.ID, leaseToken, lastResult, result.ProviderResponse))
	}
	return p.retryOrFail(ctx, ev, leaseToken, delivery, result, latency)
}

func (p *Processor) finishExhaustedBeforeProvider(ctx context.Context, ev *domain.Event, leaseToken uuid.UUID, maxAttempts uint32) error {
	realAttempts := ev.AttemptCount - 1
	if realAttempts < 0 {
		realAttempts = 0
	}
	lastResult, _ := json.Marshal(domain.LastResultFinished{
		Outcome:    domain.OutcomeFailed,
		ErrorClass: "MAX_PROVIDER_ATTEMPTS_EXHAUSTED",
		Message: fmt.Sprintf("maximum provider attempts (%d) were already exhausted before recovered delivery",
			maxAttempts),
		AttemptNumber: realAttempts,
		FinishedAt:    time.Now().UTC(),
	})
	p.debug("worker skipped provider call because recovered event exhausted attempts",
		"event_id", ev.ID,
		"claimed_attempt_count", ev.AttemptCount,
		"provider_attempts", realAttempts,
		"max_provider_attempts", maxAttempts)
	return p.settle(ctx, p.Repo.MarkFailedWithoutProviderAttempt(ctx, ev.ID, leaseToken, lastResult))
}

func (p *Processor) deferForOpenCircuit(ctx context.Context, ev *domain.Event, leaseToken uuid.UUID, delivery mq.Delivery, retryAfter time.Duration) error {
	realAttempts := ev.AttemptCount - 1
	if realAttempts < 0 {
		realAttempts = 0
	}
	baseAttempt := uint32(realAttempts)
	if baseAttempt == 0 {
		baseAttempt = 1
	}
	delay := delivery.RequeueDelayFor(baseAttempt)
	if retryAfter > delay {
		delay = retryAfter
	}
	if delay < 0 {
		delay = 0
	}
	now := time.Now().UTC()
	nextAttemptAt := now.Add(delay)
	lastResult, _ := json.Marshal(domain.LastResultFinished{
		Outcome:       domain.OutcomeRequeueRequested,
		ErrorClass:    "CIRCUIT_OPEN",
		Message:       "provider action circuit is open",
		Retryable:     true,
		NextAttemptAt: &nextAttemptAt,
		AttemptNumber: realAttempts,
		FinishedAt:    now,
	})
	recoveryDeadline := nextAttemptAt.Add(p.LeaseDuration)
	if err := p.Repo.MarkCircuitOpenWaitingForRequeue(ctx, ev.ID, leaseToken, recoveryDeadline, lastResult); err != nil {
		return p.settle(ctx, err)
	}
	p.debug("worker deferred event because provider circuit is open",
		"event_id", ev.ID,
		"provider_code", ev.ProviderCode,
		"provider_action", ev.ProviderAction,
		"attempt_number", realAttempts,
		"requeue_delay", delay,
		"next_attempt_at", nextAttemptAt)
	return mq.RequestRequeue(errors.New("provider circuit is open"), delay, false)
}

func (p *Processor) retryOrFail(ctx context.Context, ev *domain.Event, leaseToken uuid.UUID, delivery mq.Delivery, result *provider.Result, latency time.Duration) error {
	// The database count is authoritative because broker deliveries rejected
	// by an open circuit never reached the provider and are rolled back.
	persistedAttemptsExhausted := delivery.MaxAttempts > 0 && ev.AttemptCount > 0 && uint32(ev.AttemptCount) >= delivery.MaxAttempts
	if persistedAttemptsExhausted {
		lastResult, _ := json.Marshal(domain.LastResultFinished{
			Outcome: domain.OutcomeFailed, HTTPStatus: result.HTTPStatus,
			ErrorClass: "MAX_PROVIDER_ATTEMPTS_EXHAUSTED",
			Message: fmt.Sprintf("maximum provider attempts (%d) exhausted after %s: %s",
				delivery.MaxAttempts, result.ErrorClass, result.Message),
			LatencyMs: latency.Milliseconds(), AttemptNumber: ev.AttemptCount, FinishedAt: time.Now().UTC(),
		})
		p.debug("worker provider failure exhausted provider attempts",
			"event_id", ev.ID, "attempt_number", ev.AttemptCount,
			"broker_attempts", delivery.Attempts, "provider_attempts", ev.AttemptCount,
			"max_provider_attempts", delivery.MaxAttempts,
			"error_class", result.ErrorClass)
		return p.settle(ctx, p.Repo.MarkFailed(ctx, ev.ID, leaseToken, lastResult, result.ProviderResponse))
	}
	return p.releaseForRequeue(ctx, ev, leaseToken, delivery, result, latency)
}

func (p *Processor) releaseForRequeue(ctx context.Context, ev *domain.Event, leaseToken uuid.UUID, delivery mq.Delivery, result *provider.Result, latency time.Duration) error {
	now := time.Now().UTC()
	requeueDelay := delivery.RequeueDelayFor(uint32(ev.AttemptCount))
	if requeueDelay < 0 {
		requeueDelay = 0
	}
	nextAttemptAt := now.Add(requeueDelay)
	lastResult, _ := json.Marshal(domain.LastResultFinished{
		Outcome: domain.OutcomeRequeueRequested, HTTPStatus: result.HTTPStatus,
		ErrorClass: result.ErrorClass, Message: result.Message,
		Retryable: true, NextAttemptAt: &nextAttemptAt,
		LatencyMs: latency.Milliseconds(), AttemptNumber: ev.AttemptCount, FinishedAt: now,
	})
	// Keep the event PROCESSING while MQ owns the redelivery. The extra lease
	// duration is only a crash-recovery grace period; it does not delay a real
	// broker delivery, which can atomically claim the lease immediately.
	recoveryDeadline := nextAttemptAt.Add(p.LeaseDuration)
	if err := p.Repo.MarkWaitingForRequeue(ctx, ev.ID, leaseToken, recoveryDeadline, lastResult, result.ProviderResponse); err != nil {
		return p.settle(ctx, err)
	}
	p.debug("worker provider failure released for mq requeue",
		"event_id", ev.ID, "attempt_number", ev.AttemptCount,
		"requeue_delay", requeueDelay, "next_attempt_at", nextAttemptAt,
		"recovery_deadline", recoveryDeadline, "error_class", result.ErrorClass)
	return mq.RequestRequeue(fmt.Errorf("provider failure: %s", result.ErrorClass), requeueDelay, true)
}

func (p *Processor) debug(message string, args ...any) {
	if p.Logger != nil {
		p.Logger.Debug(message, args...)
	}
}

// settle turns ErrNotClaimed (the lease was lost mid-attempt, e.g. it
// expired and the compensator already reclaimed the row) into a no-op ACK
// rather than an error that would cause a pointless MQ redelivery.
func (p *Processor) settle(ctx context.Context, err error) error {
	if errors.Is(err, store.ErrNotClaimed) {
		return nil
	}
	return err
}
