package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"notification-delivery/internal/bootstrap"
	"notification-delivery/internal/domain"
	"notification-delivery/internal/store"
)

type memoryRepository struct {
	events map[string]*domain.Event
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{events: make(map[string]*domain.Event)}
}

func (r *memoryRepository) Insert(_ context.Context, event *domain.Event) error {
	key := event.SourceSystem + "/" + event.SourceRequestID
	if _, exists := r.events[key]; exists {
		return store.ErrDuplicateSourceRequest
	}
	now := time.Now().UTC()
	event.Status = domain.StatusPending
	event.CreatedAt = now
	event.UpdatedAt = now
	copy := *event
	r.events[key] = &copy
	return nil
}

func (r *memoryRepository) FindBySourceRequest(_ context.Context, sourceSystem, sourceRequestID string) (*domain.Event, error) {
	event, exists := r.events[sourceSystem+"/"+sourceRequestID]
	if !exists {
		return nil, store.ErrNotFound
	}
	copy := *event
	return &copy, nil
}

type recordingPublisher struct {
	ids []uuid.UUID
	err error
}

func (p *recordingPublisher) PublishAndMark(_ context.Context, id uuid.UUID) error {
	p.ids = append(p.ids, id)
	return p.err
}

func buildTestService(t *testing.T, publisher Publisher) (*Service, *memoryRepository) {
	t.Helper()
	t.Setenv("LARK_BOT_WEBHOOK_URL", "https://open.larksuite.com/open-apis/bot/v2/hook/test")
	registry, err := bootstrap.BuildRegistry("../../../config/providers.yaml", nil, nil)
	if err != nil {
		t.Fatalf("BuildRegistry() error = %v", err)
	}
	repo := newMemoryRepository()
	return NewService(repo, registry, publisher, nil), repo
}

func TestSubmitPersistsAndAcceptsWhenDirectPublishFails(t *testing.T) {
	publisher := &recordingPublisher{err: errors.New("broker unavailable")}
	service, repo := buildTestService(t, publisher)

	result, err := service.Submit(context.Background(), SubmitCommand{
		SourceSystem:    "billing",
		SourceRequestID: "request-1",
		ProviderCode:    "lark-bot",
		ProviderAction:  "send",
		Payload:         []byte("{\"msg_type\":\"text\",\"content\":{\"text\":\"hello\"}}"),
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if result.Status != string(domain.StatusPending) || result.Duplicate {
		t.Fatalf("Submit() result = %+v, want a new PENDING event", result)
	}
	if len(repo.events) != 1 || len(publisher.ids) != 1 {
		t.Fatalf("persisted=%d published=%d, want one attempt of each", len(repo.events), len(publisher.ids))
	}
}

func TestSubmitCanonicalDuplicateDoesNotRepublish(t *testing.T) {
	publisher := &recordingPublisher{}
	service, _ := buildTestService(t, publisher)
	first := SubmitCommand{
		SourceSystem:    "billing",
		SourceRequestID: "request-2",
		ProviderCode:    "lark-bot",
		ProviderAction:  "send",
		Payload:         []byte("{\"msg_type\":\"text\",\"content\":{\"text\":\"hello\"}}"),
	}
	firstResult, err := service.Submit(context.Background(), first)
	if err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	duplicate := first
	duplicate.Payload = []byte("{\"content\":{\"text\":\"hello\"},\"msg_type\":\"text\"}")
	duplicateResult, err := service.Submit(context.Background(), duplicate)
	if err != nil {
		t.Fatalf("duplicate Submit() error = %v", err)
	}
	if !duplicateResult.Duplicate || duplicateResult.EventID != firstResult.EventID {
		t.Fatalf("duplicate result = %+v, first = %+v", duplicateResult, firstResult)
	}
	if len(publisher.ids) != 1 {
		t.Fatalf("PublishAndMark calls = %d, want 1", len(publisher.ids))
	}
}

func TestSubmitConflictingIdempotencyKey(t *testing.T) {
	service, _ := buildTestService(t, &recordingPublisher{})
	command := SubmitCommand{
		SourceSystem:    "billing",
		SourceRequestID: "request-3",
		ProviderCode:    "lark-bot",
		ProviderAction:  "send",
		Payload:         []byte("{\"msg_type\":\"text\",\"content\":{\"text\":\"first\"}}"),
	}
	if _, err := service.Submit(context.Background(), command); err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	command.Payload = []byte("{\"msg_type\":\"text\",\"content\":{\"text\":\"different\"}}")
	if _, err := service.Submit(context.Background(), command); !errors.Is(err, ErrSourceRequestConflict) {
		t.Fatalf("conflicting Submit() error = %v, want ErrSourceRequestConflict", err)
	}
}
