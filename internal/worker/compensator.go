package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"notification-delivery/internal/config"
	"notification-delivery/internal/domain"
	"notification-delivery/internal/publish"
	"notification-delivery/internal/store"
)

// Compensator runs the two background scans described in DESIGN.md §8 and
// §9. It is embedded in every worker process: each replica scans
// independently and the SQL's WHERE-guarded UPDATEs make concurrent scans
// across replicas safe — at most one wins each row, the rest affect zero
// rows.
type Compensator struct {
	Repo          *store.EventRepo
	Publisher     *publish.Service
	Logger        *slog.Logger
	Cfg           config.CompensatorConfig
	LeaseDuration time.Duration
}

func (c *Compensator) Run(ctx context.Context) {
	go c.loop(ctx, c.Cfg.PublishScanInterval, c.scanPendingPublish)
	go c.loop(ctx, c.Cfg.LeaseScanInterval, c.scanExpiredLeases)
}

func (c *Compensator) loop(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	// Run once at startup so events left behind by a process restart do not
	// wait for the first ticker interval before they make progress.
	fn(ctx)
	if ctx.Err() != nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn(ctx)
		}
	}
}

// scanPendingPublish is the fallback path for DESIGN.md §8: rows the API's
// direct post-commit publish never made it to the broker for (API crash,
// broker hiccup). On the happy path this finds nothing.
func (c *Compensator) scanPendingPublish(ctx context.Context) {
	limit := c.Cfg.PublishBatchSize
	if limit <= 0 {
		limit = 500
	}
	ids, err := c.Repo.SelectPendingToPublish(ctx, limit)
	if err != nil {
		c.Logger.Error("compensator: select pending to publish failed", "error", err)
		return
	}
	c.debug("compensator pending publish scan completed", "event_count", len(ids), "limit", limit)
	for _, id := range ids {
		if err := c.Publisher.PublishAndMark(ctx, id); err != nil {
			c.Logger.Warn("compensator: republish failed, will retry next scan", "event_id", id, "error", err)
		} else {
			c.debug("compensator republish completed", "event_id", id)
		}
	}
}

// scanExpiredLeases implements DESIGN.md §9: a worker that claimed a row
// and then crashed (or was killed) leaves it PROCESSING forever unless
// something notices the lease expired. Recovery uses the same requeue path as
// an ordinary provider failure.
func (c *Compensator) scanExpiredLeases(ctx context.Context) {
	limit := c.Cfg.LeaseScanBatchSize
	if limit <= 0 {
		limit = 200
	}
	events, err := c.Repo.SelectExpiredLeases(ctx, limit)
	if err != nil {
		c.Logger.Error("compensator: select expired leases failed", "error", err)
		return
	}
	c.debug("compensator expired lease scan completed", "event_count", len(events), "limit", limit)
	for _, ev := range events {
		c.recoverOne(ctx, ev)
	}
}

func (c *Compensator) recoverOne(ctx context.Context, ev *domain.Event) {
	if ev.LeaseToken == nil && outcomeOf(ev.LastResult) == domain.OutcomeRequeueRequested {
		leaseDuration := c.LeaseDuration
		if leaseDuration <= 0 {
			leaseDuration = 60 * time.Second
		}
		if err := c.Repo.ReserveExpiredRequeue(ctx, ev.ID, leaseDuration); err != nil {
			if !errors.Is(err, store.ErrNotClaimed) {
				c.Logger.Error("compensator: reserve expired mq requeue failed", "event_id", ev.ID, "error", err)
			}
			return
		}
		if err := c.Publisher.PublishAndMark(ctx, ev.ID); err != nil {
			c.Logger.Warn("compensator: recover mq requeue publish failed", "event_id", ev.ID, "error", err)
			return
		}
		c.debug("compensator recovered lost mq requeue", "event_id", ev.ID)
		return
	}
	if ev.LeaseToken == nil {
		return
	}
	leaseDuration := c.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = 60 * time.Second
	}
	now := time.Now().UTC()
	requestMayHaveBeenSent := phaseOf(ev.LastResult) == domain.PhaseRequesting
	errorClass := "WORKER_LEASE_EXPIRED"
	message := "worker lease expired before the provider request completed"
	if requestMayHaveBeenSent {
		errorClass = "WORKER_LEASE_EXPIRED_DURING_REQUEST"
		message = "worker lease expired while an outbound request may have been in flight"
	}
	result, _ := json.Marshal(domain.LastResultFinished{
		Outcome:                domain.OutcomeRequeueRequested,
		ErrorClass:             errorClass,
		Message:                message,
		RequestMayHaveBeenSent: requestMayHaveBeenSent,
		Retryable:              true,
		NextAttemptAt:          &now,
		AttemptNumber:          ev.AttemptCount,
		FinishedAt:             now,
	})
	if err := c.Repo.RecoverExpiredToRequeue(ctx, ev.ID, *ev.LeaseToken, now.Add(leaseDuration), result); err != nil {
		if !errors.Is(err, store.ErrNotClaimed) {
			c.Logger.Error("compensator: recover expired worker lease failed", "event_id", ev.ID, "error", err)
		}
		return
	}
	if err := c.Publisher.PublishAndMark(ctx, ev.ID); err != nil {
		c.Logger.Warn("compensator: publish recovered worker lease failed", "event_id", ev.ID, "error", err)
		return
	}
	c.debug("compensator recovered expired worker lease for mq requeue", "event_id", ev.ID, "request_may_have_been_sent", requestMayHaveBeenSent)
}

func (c *Compensator) debug(message string, args ...any) {
	if c.Logger != nil {
		c.Logger.Debug(message, args...)
	}
}

func phaseOf(lastResult json.RawMessage) string {
	if len(lastResult) == 0 {
		return domain.PhaseClaimed
	}
	var v struct {
		Phase string `json:"phase"`
	}
	if err := json.Unmarshal(lastResult, &v); err != nil {
		return domain.PhaseClaimed
	}
	if v.Phase == domain.PhaseRequesting {
		return domain.PhaseRequesting
	}
	return domain.PhaseClaimed
}

func outcomeOf(lastResult json.RawMessage) string {
	var v struct {
		Outcome string `json:"outcome"`
	}
	if json.Unmarshal(lastResult, &v) != nil {
		return ""
	}
	return v.Outcome
}
