package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"

	"notification-delivery/internal/domain"
)

const eventColumns = `
	id, source_system, source_request_id, provider_code, provider_action,
	payload, status, attempt_count, next_attempt_at, enqueued_at,
	lease_token, lease_until, last_result, provider_response,
	created_at, updated_at`

// EventRepo is the sole data-access surface for notification_event. Every
// state transition documented in DESIGN.md §6 has one method here; the SQL
// bodies are copied verbatim from the design doc so behavior stays
// traceable back to the spec.
type EventRepo struct {
	db *gorm.DB
}

func NewEventRepo(db *gorm.DB) *EventRepo {
	return &EventRepo{db: db}
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanEvent(scanner rowScanner) (*domain.Event, error) {
	var ev domain.Event
	var idStr string
	var leaseToken *string
	var status string
	// Scan nullable JSON columns into the database/sql native []byte type.
	// json.RawMessage is a named []byte type and cannot receive a SQL NULL
	// directly (database/sql reports "unsupported Scan ... <nil>").
	var payload []byte
	var lastResult []byte
	var providerResponse []byte
	if err := scanner.Scan(
		&idStr, &ev.SourceSystem, &ev.SourceRequestID, &ev.ProviderCode, &ev.ProviderAction,
		&payload, &status, &ev.AttemptCount, &ev.NextAttemptAt, &ev.EnqueuedAt,
		&leaseToken, &ev.LeaseUntil, &lastResult, &providerResponse,
		&ev.CreatedAt, &ev.UpdatedAt,
	); err != nil {
		return nil, err
	}
	ev.Payload = json.RawMessage(payload)
	ev.LastResult = json.RawMessage(lastResult)
	ev.ProviderResponse = json.RawMessage(providerResponse)
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, err
	}
	ev.ID = id
	ev.Status = domain.Status(status)
	if leaseToken != nil {
		lt, err := uuid.Parse(*leaseToken)
		if err != nil {
			return nil, err
		}
		ev.LeaseToken = &lt
	}
	return &ev, nil
}

func isDuplicateSourceRequestError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true
	}
	return false
}

// Insert accepts a new PENDING event. Returns ErrDuplicateSourceRequest on
// a (source_system, source_request_id) unique-constraint violation; the
// caller re-reads the existing row via FindBySourceRequest to decide
// between an idempotent-duplicate response and a 409 conflict.
func (r *EventRepo) Insert(ctx context.Context, ev *domain.Event) error {
	row := r.db.WithContext(ctx).Raw(`
		INSERT INTO notification_event (
			id, source_system, source_request_id, provider_code, provider_action,
			payload, status, attempt_count, next_attempt_at
		) VALUES (
			$1::uuid, $2, $3, $4, $5, $6::jsonb, $7, 0, now()
		)
		RETURNING created_at, updated_at`,
		ev.ID.String(), ev.SourceSystem, ev.SourceRequestID, ev.ProviderCode, ev.ProviderAction,
		[]byte(ev.Payload), string(domain.StatusPending),
	).Row()

	if err := row.Scan(&ev.CreatedAt, &ev.UpdatedAt); err != nil {
		if isDuplicateSourceRequestError(err) {
			return ErrDuplicateSourceRequest
		}
		return err
	}
	ev.Status = domain.StatusPending
	return nil
}

// FindBySourceRequest looks up a row by the complete internal idempotency key.
func (r *EventRepo) FindBySourceRequest(ctx context.Context, sourceSystem, sourceRequestID string) (*domain.Event, error) {
	row := r.db.WithContext(ctx).Raw(`SELECT `+eventColumns+`
		FROM notification_event
		WHERE source_system = $1 AND source_request_id = $2`,
		sourceSystem, sourceRequestID).Row()
	ev, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return ev, nil
}

func (r *EventRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Event, error) {
	row := r.db.WithContext(ctx).Raw(`SELECT `+eventColumns+`
		FROM notification_event WHERE id = $1::uuid`, id.String()).Row()
	ev, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return ev, nil
}

// --- §8 publish / compensation -------------------------------------------------

// SelectPendingToPublish finds rows the Publisher has not yet confirmed to
// the broker. On the happy path this is empty: the API publishes
// synchronously right after commit. It only finds work when that direct
// publish was lost (API crash, broker hiccup).
func (r *EventRepo) SelectPendingToPublish(ctx context.Context, limit int) ([]uuid.UUID, error) {
	rows, err := r.db.WithContext(ctx).Raw(`
		SELECT id FROM notification_event
		WHERE status = 'PENDING'
		  AND next_attempt_at <= now()
		  AND enqueued_at IS NULL
		ORDER BY next_attempt_at
		LIMIT $1`, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// MarkEnqueued records that the broker acknowledged the publish. Guarded by
// status='PENDING' so a row that a worker has already claimed in the
// meantime is left untouched.
func (r *EventRepo) MarkEnqueued(ctx context.Context, id uuid.UUID) error {
	return r.db.WithContext(ctx).Exec(`
		UPDATE notification_event
		SET enqueued_at = now(), updated_at = now()
		WHERE id = $1::uuid AND status = 'PENDING'`, id.String()).Error
}

// --- §6.1 atomic claim -------------------------------------------------

// Claim either performs the initial PENDING -> PROCESSING transition or
// atomically resumes a PROCESSING event that is waiting for MQ redelivery.
// A waiting event has no lease_token and a REQUEUE_REQUESTED last result.
// attempt_count is incremented only when a delivery actually wins a lease.
func (r *EventRepo) Claim(ctx context.Context, id, leaseToken uuid.UUID, leaseDuration time.Duration) (*domain.Event, error) {
	claimed := domain.LastResultClaimed{Phase: domain.PhaseClaimed, ClaimedAt: time.Now().UTC()}
	claimedJSON, err := json.Marshal(claimed)
	if err != nil {
		return nil, err
	}
	row := r.db.WithContext(ctx).Raw(`
		UPDATE notification_event
		SET
			status = 'PROCESSING',
			attempt_count = attempt_count + 1,
			lease_token = $2::uuid,
			lease_until = now() + $3 * interval '1 second',
			last_result = $4::jsonb,
			provider_response = NULL,
			updated_at = now()
		WHERE id = $1::uuid
		  AND (
		        (status = 'PENDING' AND next_attempt_at <= now())
		        OR (
		            status = 'PROCESSING'
		            AND lease_token IS NULL
		            AND last_result->>'outcome' = 'REQUEUE_REQUESTED'
		        )
		      )
		RETURNING `+eventColumns,
		id.String(), leaseToken.String(), leaseDuration.Seconds(), claimedJSON,
	).Row()
	ev, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotClaimed
		}
		return nil, err
	}
	return ev, nil
}

// RenewLease extends lease_until for long-running deliveries. Matches on
// lease_token so a worker that lost its lease (e.g. paused past
// lease_until and got reclaimed) cannot resurrect it.
func (r *EventRepo) RenewLease(ctx context.Context, id, leaseToken uuid.UUID, extendBy time.Duration) error {
	tx := r.db.WithContext(ctx).Exec(`
		UPDATE notification_event
		SET lease_until = now() + $3 * interval '1 second', updated_at = now()
		WHERE id = $1::uuid AND status = 'PROCESSING' AND lease_token = $2::uuid`,
		id.String(), leaseToken.String(), extendBy.Seconds())
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return ErrNotClaimed
	}
	return nil
}

// --- §6.2 pre-request marker -------------------------------------------------

func (r *EventRepo) MarkRequesting(ctx context.Context, id, leaseToken uuid.UUID) error {
	requesting := domain.LastResultRequesting{Phase: domain.PhaseRequesting, StartedAt: time.Now().UTC()}
	body, err := json.Marshal(requesting)
	if err != nil {
		return err
	}
	tx := r.db.WithContext(ctx).Exec(`
		UPDATE notification_event
		SET last_result = $3::jsonb, updated_at = now()
		WHERE id = $1::uuid AND status = 'PROCESSING' AND lease_token = $2::uuid`,
		id.String(), leaseToken.String(), body)
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return ErrNotClaimed
	}
	return nil
}

// --- §6.3-6.6 attempt-result transitions -------------------------------------------

func (r *EventRepo) MarkSucceeded(ctx context.Context, id, leaseToken uuid.UUID, result, providerResponse json.RawMessage) error {
	return r.transition(ctx, id, leaseToken, `
		UPDATE notification_event
		SET
			status = 'SUCCEEDED',
			lease_token = NULL,
			lease_until = NULL,
			last_result = $3::jsonb,
			provider_response = $4::jsonb,
			updated_at = now()
		WHERE id = $1::uuid AND status = 'PROCESSING' AND lease_token = $2::uuid`,
		result, providerResponse)
}

// MarkWaitingForRequeue completes one failed attempt without changing
// the event's PROCESSING lifecycle state. Clearing lease_token distinguishes
// an MQ-owned requeue wait from an actively executing worker. leaseUntil is a
// recovery deadline; the broker's delivery timing remains authoritative.
func (r *EventRepo) MarkWaitingForRequeue(ctx context.Context, id, leaseToken uuid.UUID, leaseUntil time.Time, result, providerResponse json.RawMessage) error {
	return r.transition(ctx, id, leaseToken, `
		UPDATE notification_event
		SET
			lease_token = NULL,
			lease_until = $3,
			last_result = $4::jsonb,
			provider_response = $5::jsonb,
			updated_at = now()
		WHERE id = $1::uuid AND status = 'PROCESSING' AND lease_token = $2::uuid`,
		leaseUntil, result, providerResponse)
}

// MarkCircuitOpenWaitingForRequeue releases a claim that was rejected by an
// open circuit before any provider request started. Claim increments
// attempt_count, so this guarded transition rolls that increment back and
// keeps the event in the existing lease-free MQ requeue substate.
func (r *EventRepo) MarkCircuitOpenWaitingForRequeue(ctx context.Context, id, leaseToken uuid.UUID, leaseUntil time.Time, result json.RawMessage) error {
	return r.transition(ctx, id, leaseToken, `
		UPDATE notification_event
		SET
			attempt_count = GREATEST(attempt_count - 1, 0),
			lease_token = NULL,
			lease_until = $3,
			last_result = $4::jsonb,
			provider_response = NULL,
			updated_at = now()
		WHERE id = $1::uuid AND status = 'PROCESSING' AND lease_token = $2::uuid`,
		leaseUntil, result)
}

// ReserveExpiredRequeue gives one compensator instance a bounded window to
// republish a PROCESSING event whose broker wake-up was lost. The event stays
// in the lease-free REQUEUE_REQUESTED substate so a real delivery can claim it
// immediately, even while this recovery deadline is in the future.
func (r *EventRepo) ReserveExpiredRequeue(ctx context.Context, id uuid.UUID, extendBy time.Duration) error {
	tx := r.db.WithContext(ctx).Exec(`
		UPDATE notification_event
		SET lease_until = now() + $2 * interval '1 second', updated_at = now()
		WHERE id = $1::uuid
		  AND status = 'PROCESSING'
		  AND lease_token IS NULL
		  AND lease_until < now()
		  AND last_result->>'outcome' = 'REQUEUE_REQUESTED'`,
		id.String(), extendBy.Seconds())
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return ErrNotClaimed
	}
	return nil
}

func (r *EventRepo) MarkFailed(ctx context.Context, id, leaseToken uuid.UUID, result, providerResponse json.RawMessage) error {
	return r.transition(ctx, id, leaseToken, `
		UPDATE notification_event
		SET
			status = 'FAILED',
			lease_token = NULL,
			lease_until = NULL,
			last_result = $3::jsonb,
			provider_response = $4::jsonb,
			updated_at = now()
		WHERE id = $1::uuid AND status = 'PROCESSING' AND lease_token = $2::uuid`,
		result, providerResponse)
}

// MarkFailedWithoutProviderAttempt terminates a recovered delivery that was
// claimed after the provider-attempt budget had already been exhausted. Claim
// increments attempt_count, so this transition rolls back that increment to
// keep the column equal to the number of real provider calls.
func (r *EventRepo) MarkFailedWithoutProviderAttempt(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) error {
	return r.transition(ctx, id, leaseToken, `
		UPDATE notification_event
		SET
			status = 'FAILED',
			attempt_count = GREATEST(attempt_count - 1, 0),
			lease_token = NULL,
			lease_until = NULL,
			last_result = $3::jsonb,
			provider_response = NULL,
			updated_at = now()
		WHERE id = $1::uuid AND status = 'PROCESSING' AND lease_token = $2::uuid`,
		result)
}

func (r *EventRepo) transition(ctx context.Context, id, leaseToken uuid.UUID, sql string, args ...any) error {
	params := []any{id.String(), leaseToken.String()}
	params = append(params, args...)
	tx := r.db.WithContext(ctx).Exec(sql, params...)
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return ErrNotClaimed
	}
	return nil
}

// --- §9 worker crash recovery -------------------------------------------------

// SelectExpiredLeases finds PROCESSING rows whose active worker lease or
// lease-free MQ-requeue recovery deadline has expired.
func (r *EventRepo) SelectExpiredLeases(ctx context.Context, limit int) ([]*domain.Event, error) {
	rows, err := r.db.WithContext(ctx).Raw(`SELECT `+eventColumns+`
		FROM notification_event
		WHERE status = 'PROCESSING' AND lease_until < now()
		ORDER BY lease_until
		LIMIT $1`, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []*domain.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// RecoverExpiredToRequeue releases an expired active worker lease into the
// same PROCESSING + REQUEUE_REQUESTED state used by ordinary provider errors.
// The caller republishes the event after this guarded update succeeds.
func (r *EventRepo) RecoverExpiredToRequeue(ctx context.Context, id, leaseToken uuid.UUID, recoveryDeadline time.Time, result json.RawMessage) error {
	tx := r.db.WithContext(ctx).Exec(`
		UPDATE notification_event
		SET
			lease_token = NULL,
			lease_until = $3,
			last_result = $4::jsonb,
			provider_response = NULL,
			updated_at = now()
		WHERE id = $1::uuid AND status = 'PROCESSING' AND lease_token = $2::uuid AND lease_until < now()`,
		id.String(), leaseToken.String(), recoveryDeadline, result)
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return ErrNotClaimed
	}
	return nil
}
