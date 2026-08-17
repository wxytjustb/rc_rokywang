package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"notification-delivery/internal/domain"
)

type nullableJSONRow struct{}

func (nullableJSONRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	*(dest[1].(*string)) = "example-system"
	*(dest[2].(*string)) = "request-1"
	*(dest[3].(*string)) = "lark-bot"
	*(dest[4].(*string)) = "send"
	*(dest[5].(*[]byte)) = []byte(`{"msg_type":"text"}`)
	*(dest[6].(*string)) = "PROCESSING"
	*(dest[7].(*int16)) = 1
	now := time.Now().UTC()
	*(dest[8].(*time.Time)) = now
	*(dest[12].(*[]byte)) = []byte(`{"phase":"CLAIMED"}`)
	*(dest[13].(*[]byte)) = nil
	*(dest[14].(*time.Time)) = now
	*(dest[15].(*time.Time)) = now
	return nil
}

func TestScanEventAcceptsNullProviderResponse(t *testing.T) {
	ev, err := scanEvent(nullableJSONRow{})
	if err != nil {
		t.Fatalf("scanEvent() error = %v", err)
	}
	if string(ev.Payload) != `{"msg_type":"text"}` {
		t.Errorf("payload = %s", ev.Payload)
	}
	if string(ev.LastResult) != `{"phase":"CLAIMED"}` {
		t.Errorf("last_result = %s", ev.LastResult)
	}
	if ev.ProviderResponse != nil {
		t.Errorf("provider_response = %s, want nil", ev.ProviderResponse)
	}
}

func TestClaimConcurrentIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}

	const claimers = 16
	ctx := context.Background()
	db, err := NewPool(ctx, dsn, claimers, 0, "postgres")
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("DB() error = %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	tests := []struct {
		name            string
		prepareRequeue  bool
		attemptsAtStart int16
	}{
		{name: "pending", attemptsAtStart: 0},
		{name: "requeue_requested", prepareRequeue: true, attemptsAtStart: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := NewEventRepo(db)
			ev := &domain.Event{
				ID: uuid.New(), SourceSystem: "claim-concurrency-integration",
				SourceRequestID: uuid.NewString(), ProviderCode: "lark-bot",
				ProviderAction: "send", Payload: json.RawMessage(`{"msg_type":"text","content":{"text":"test"}}`),
			}
			if err := repo.Insert(ctx, ev); err != nil {
				t.Fatalf("Insert() error = %v", err)
			}
			t.Cleanup(func() {
				_ = db.Exec("DELETE FROM notification_event WHERE id = $1::uuid", ev.ID.String()).Error
			})

			if tt.prepareRequeue {
				lease := uuid.New()
				if _, err := repo.Claim(ctx, ev.ID, lease, time.Minute); err != nil {
					t.Fatalf("prepare Claim() error = %v", err)
				}
				result := json.RawMessage(`{"outcome":"REQUEUE_REQUESTED"}`)
				if err := repo.MarkWaitingForRequeue(ctx, ev.ID, lease, time.Now().UTC().Add(time.Minute), result, nil); err != nil {
					t.Fatalf("prepare MarkWaitingForRequeue() error = %v", err)
				}
			}

			type claimResult struct {
				lease uuid.UUID
				event *domain.Event
				err   error
			}
			start := make(chan struct{})
			results := make(chan claimResult, claimers)
			var ready sync.WaitGroup
			ready.Add(claimers)
			for range claimers {
				lease := uuid.New()
				go func() {
					ready.Done()
					<-start
					claimed, err := repo.Claim(ctx, ev.ID, lease, time.Minute)
					results <- claimResult{lease: lease, event: claimed, err: err}
				}()
			}
			ready.Wait()
			close(start)

			var winner claimResult
			successes := 0
			notClaimed := 0
			for range claimers {
				result := <-results
				switch {
				case result.err == nil:
					successes++
					winner = result
				case errors.Is(result.err, ErrNotClaimed):
					notClaimed++
				default:
					t.Errorf("Claim() unexpected error = %v", result.err)
				}
			}
			if successes != 1 || notClaimed != claimers-1 {
				t.Fatalf("concurrent Claim() successes=%d not_claimed=%d, want 1 and %d", successes, notClaimed, claimers-1)
			}
			if winner.event.LeaseToken == nil || *winner.event.LeaseToken != winner.lease {
				t.Fatalf("winning Claim() lease_token=%v, want %v", winner.event.LeaseToken, winner.lease)
			}

			stored, err := repo.GetByID(ctx, ev.ID)
			if err != nil {
				t.Fatalf("GetByID() error = %v", err)
			}
			if stored.Status != domain.StatusProcessing || stored.AttemptCount != tt.attemptsAtStart+1 {
				t.Fatalf("stored event status=%s attempts=%d, want PROCESSING and %d", stored.Status, stored.AttemptCount, tt.attemptsAtStart+1)
			}
			if stored.LeaseToken == nil || *stored.LeaseToken != winner.lease {
				t.Fatalf("stored lease_token=%v, want winning lease %v", stored.LeaseToken, winner.lease)
			}
		})
	}
}

func TestProcessingRequeueLeaseIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx := context.Background()
	db, err := NewPool(ctx, dsn, 2, 0, "postgres")
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("DB() error = %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	repo := NewEventRepo(db)
	ev := &domain.Event{
		ID: uuid.New(), SourceSystem: "requeue-integration",
		SourceRequestID: uuid.NewString(), ProviderCode: "lark-bot",
		ProviderAction: "send", Payload: json.RawMessage(`{"msg_type":"text","content":{"text":"test"}}`),
	}
	if err := repo.Insert(ctx, ev); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Exec("DELETE FROM notification_event WHERE id = $1::uuid", ev.ID.String()).Error
	})
	if err := repo.MarkEnqueued(ctx, ev.ID); err != nil {
		t.Fatalf("MarkEnqueued() error = %v", err)
	}

	firstLease := uuid.New()
	first, err := repo.Claim(ctx, ev.ID, firstLease, time.Minute)
	if err != nil {
		t.Fatalf("first Claim() error = %v", err)
	}
	recoveryDeadline := time.Now().UTC().Add(time.Minute)
	result := json.RawMessage(`{"outcome":"REQUEUE_REQUESTED"}`)
	response := json.RawMessage(`{"http_status":429}`)
	if err := repo.MarkWaitingForRequeue(ctx, ev.ID, firstLease, recoveryDeadline, result, response); err != nil {
		t.Fatalf("MarkWaitingForRequeue() error = %v", err)
	}

	waiting, err := repo.GetByID(ctx, ev.ID)
	if err != nil {
		t.Fatalf("GetByID(waiting) error = %v", err)
	}
	if waiting.Status != domain.StatusProcessing || waiting.AttemptCount != first.AttemptCount {
		t.Fatalf("waiting event status=%s attempts=%d", waiting.Status, waiting.AttemptCount)
	}
	if waiting.LeaseToken != nil || waiting.LeaseUntil == nil {
		t.Fatalf("waiting event lease state = %+v", waiting)
	}
	if waiting.EnqueuedAt == nil {
		t.Fatal("waiting event unexpectedly cleared enqueued_at")
	}
	if !waiting.NextAttemptAt.Equal(first.NextAttemptAt) {
		t.Fatalf("next_attempt_at changed from %v to %v", first.NextAttemptAt, waiting.NextAttemptAt)
	}
	if !sameJSON(waiting.LastResult, result) || !sameJSON(waiting.ProviderResponse, response) {
		t.Fatalf("waiting result=%s provider_response=%s", waiting.LastResult, waiting.ProviderResponse)
	}

	secondLease := uuid.New()
	second, err := repo.Claim(ctx, ev.ID, secondLease, time.Minute)
	if err != nil {
		t.Fatalf("requeue Claim() error = %v", err)
	}
	if second.Status != domain.StatusProcessing || second.AttemptCount != first.AttemptCount+1 {
		t.Fatalf("requeue claim status=%s attempts=%d", second.Status, second.AttemptCount)
	}
	if second.LeaseToken == nil || *second.LeaseToken != secondLease {
		t.Fatalf("requeue claim lease_token=%v, want %v", second.LeaseToken, secondLease)
	}
	if second.ProviderResponse != nil {
		t.Fatalf("requeue claim retained provider_response=%s", second.ProviderResponse)
	}

	if err := repo.MarkWaitingForRequeue(ctx, ev.ID, secondLease, time.Now().UTC().Add(-time.Second), result, response); err != nil {
		t.Fatalf("MarkWaitingForRequeue(expired) error = %v", err)
	}
	if err := repo.ReserveExpiredRequeue(ctx, ev.ID, time.Minute); err != nil {
		t.Fatalf("ReserveExpiredRequeue() error = %v", err)
	}
	reserved, err := repo.GetByID(ctx, ev.ID)
	if err != nil {
		t.Fatalf("GetByID(reserved) error = %v", err)
	}
	if reserved.Status != domain.StatusProcessing || reserved.LeaseToken != nil || reserved.LeaseUntil == nil || !reserved.LeaseUntil.After(time.Now()) {
		t.Fatalf("reserved requeue state = %+v", reserved)
	}

	thirdLease := uuid.New()
	third, err := repo.Claim(ctx, ev.ID, thirdLease, time.Millisecond)
	if err != nil {
		t.Fatalf("third Claim() error = %v", err)
	}
	if err := db.Exec("UPDATE notification_event SET lease_until = now() - interval '1 second' WHERE id = $1::uuid", ev.ID.String()).Error; err != nil {
		t.Fatalf("expire active lease: %v", err)
	}
	crashResult := json.RawMessage(`{"outcome":"REQUEUE_REQUESTED","error_class":"WORKER_LEASE_EXPIRED"}`)
	if err := repo.RecoverExpiredToRequeue(ctx, ev.ID, thirdLease, time.Now().UTC().Add(time.Minute), crashResult); err != nil {
		t.Fatalf("RecoverExpiredToRequeue() error = %v", err)
	}
	recovered, err := repo.GetByID(ctx, ev.ID)
	if err != nil {
		t.Fatalf("GetByID(recovered) error = %v", err)
	}
	if recovered.Status != domain.StatusProcessing || recovered.AttemptCount != third.AttemptCount || recovered.LeaseToken != nil || recovered.LeaseUntil == nil {
		t.Fatalf("recovered active lease state = %+v", recovered)
	}
	if !sameJSON(recovered.LastResult, crashResult) || recovered.ProviderResponse != nil {
		t.Fatalf("recovered result=%s provider_response=%s", recovered.LastResult, recovered.ProviderResponse)
	}

	fourthLease := uuid.New()
	fourth, err := repo.Claim(ctx, ev.ID, fourthLease, time.Minute)
	if err != nil {
		t.Fatalf("fourth Claim() error = %v", err)
	}
	if fourth.AttemptCount != third.AttemptCount+1 {
		t.Fatalf("fourth claim attempts=%d, want %d", fourth.AttemptCount, third.AttemptCount+1)
	}
	exhaustedResult := json.RawMessage(`{"outcome":"FAILED","error_class":"MAX_PROVIDER_ATTEMPTS_EXHAUSTED","attempt_number":3}`)
	if err := repo.MarkFailedWithoutProviderAttempt(ctx, ev.ID, fourthLease, exhaustedResult); err != nil {
		t.Fatalf("MarkFailedWithoutProviderAttempt() error = %v", err)
	}
	exhausted, err := repo.GetByID(ctx, ev.ID)
	if err != nil {
		t.Fatalf("GetByID(exhausted) error = %v", err)
	}
	if exhausted.Status != domain.StatusFailed || exhausted.AttemptCount != third.AttemptCount || exhausted.LeaseToken != nil || exhausted.LeaseUntil != nil {
		t.Fatalf("exhausted recovered state = %+v", exhausted)
	}
	if !sameJSON(exhausted.LastResult, exhaustedResult) || exhausted.ProviderResponse != nil {
		t.Fatalf("exhausted result=%s provider_response=%s", exhausted.LastResult, exhausted.ProviderResponse)
	}
}

func TestCircuitOpenRequeueRollsBackProviderAttemptIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx := context.Background()
	db, err := NewPool(ctx, dsn, 2, 0, "postgres")
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("DB() error = %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	repo := NewEventRepo(db)
	ev := &domain.Event{
		ID: uuid.New(), SourceSystem: "circuit-integration",
		SourceRequestID: uuid.NewString(), ProviderCode: "lark-bot",
		ProviderAction: "send", Payload: json.RawMessage(`{"msg_type":"text","content":{"text":"test"}}`),
	}
	if err := repo.Insert(ctx, ev); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Exec("DELETE FROM notification_event WHERE id = $1::uuid", ev.ID.String()).Error
	})

	lease := uuid.New()
	claimed, err := repo.Claim(ctx, ev.ID, lease, time.Minute)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if claimed.AttemptCount != 1 {
		t.Fatalf("claimed attempts = %d, want 1", claimed.AttemptCount)
	}
	result := json.RawMessage(`{"outcome":"REQUEUE_REQUESTED","error_class":"CIRCUIT_OPEN","attempt_number":0}`)
	if err := repo.MarkCircuitOpenWaitingForRequeue(ctx, ev.ID, lease, time.Now().Add(time.Minute), result); err != nil {
		t.Fatalf("MarkCircuitOpenWaitingForRequeue() error = %v", err)
	}
	waiting, err := repo.GetByID(ctx, ev.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if waiting.AttemptCount != 0 || waiting.Status != domain.StatusProcessing || waiting.LeaseToken != nil || waiting.LeaseUntil == nil {
		t.Fatalf("circuit waiting state = %+v", waiting)
	}
	if !sameJSON(waiting.LastResult, result) || waiting.ProviderResponse != nil {
		t.Fatalf("circuit waiting result=%s provider_response=%s", waiting.LastResult, waiting.ProviderResponse)
	}

	reclaimed, err := repo.Claim(ctx, ev.ID, uuid.New(), time.Minute)
	if err != nil {
		t.Fatalf("requeue Claim() error = %v", err)
	}
	if reclaimed.AttemptCount != 1 {
		t.Fatalf("reclaimed attempts = %d, want 1", reclaimed.AttemptCount)
	}
}

func sameJSON(left, right json.RawMessage) bool {
	var leftValue any
	var rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}
