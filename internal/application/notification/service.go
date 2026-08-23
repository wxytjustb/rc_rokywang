package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/google/uuid"

	"notification-delivery/internal/domain"
	"notification-delivery/internal/provider"
	"notification-delivery/internal/store"
)

type Repository interface {
	Insert(ctx context.Context, event *domain.Event) error
	FindBySourceRequest(ctx context.Context, sourceSystem, sourceRequestID string) (*domain.Event, error)
}

type Publisher interface {
	PublishAndMark(ctx context.Context, id uuid.UUID) error
}

type Service struct {
	repo      Repository
	registry  *provider.Registry
	publisher Publisher
	logger    *slog.Logger
}

func NewService(repo Repository, registry *provider.Registry, publisher Publisher, logger *slog.Logger) *Service {
	return &Service{repo: repo, registry: registry, publisher: publisher, logger: logger}
}

func (s *Service) Submit(ctx context.Context, cmd SubmitCommand) (*SubmitResult, error) {
	if cmd.SourceSystem == "" || cmd.SourceRequestID == "" || cmd.ProviderCode == "" || cmd.ProviderAction == "" || len(cmd.Payload) == 0 || !json.Valid(cmd.Payload) {
		return nil, ErrInvalidRequest
	}
	resolved, ok := s.registry.Lookup(cmd.ProviderCode, cmd.ProviderAction)
	if !ok {
		return nil, ErrUnsupportedProviderAction
	}
	if err := resolved.Adapter.Validate(cmd.ProviderAction, cmd.Payload); err != nil {
		var validationErr *provider.ValidationError
		if errors.As(err, &validationErr) {
			return nil, &PayloadValidationError{Problems: append([]string(nil), validationErr.Problems...)}
		}
		return nil, ErrInvalidPayload
	}

	if result, found, err := s.resolveExisting(ctx, cmd); err != nil {
		return nil, err
	} else if found {
		return result, nil
	}

	event := &domain.Event{
		ID:              uuid.New(),
		SourceSystem:    cmd.SourceSystem,
		SourceRequestID: cmd.SourceRequestID,
		ProviderCode:    cmd.ProviderCode,
		ProviderAction:  cmd.ProviderAction,
		Payload:         append(json.RawMessage(nil), cmd.Payload...),
	}
	if err := s.repo.Insert(ctx, event); err != nil {
		if errors.Is(err, store.ErrDuplicateSourceRequest) {
			if result, found, resolveErr := s.resolveExisting(ctx, cmd); resolveErr != nil {
				return nil, resolveErr
			} else if found {
				return result, nil
			}
		}
		s.logError("insert notification_event failed", "error", err)
		return nil, fmt.Errorf("%w: insert notification", ErrStorageUnavailable)
	}

	if s.logger != nil {
		s.logger.DebugContext(ctx, "notification event persisted",
			"event_id", event.ID,
			"source_system", event.SourceSystem,
			"source_request_id", event.SourceRequestID,
			"provider_code", event.ProviderCode,
			"provider_action", event.ProviderAction)
	}

	// Publishing is deliberately best effort. The compensator republishes
	// persisted PENDING rows whose enqueued_at remains NULL.
	if err := s.publisher.PublishAndMark(ctx, event.ID); err != nil {
		if s.logger != nil {
			s.logger.WarnContext(ctx, "direct publish failed, relying on compensator", "event_id", event.ID, "error", err)
		}
	} else if s.logger != nil {
		s.logger.DebugContext(ctx, "direct publish completed", "event_id", event.ID)
	}

	return submitResult(event, false), nil
}

func (s *Service) resolveExisting(ctx context.Context, cmd SubmitCommand) (*SubmitResult, bool, error) {
	existing, err := s.repo.FindBySourceRequest(ctx, cmd.SourceSystem, cmd.SourceRequestID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, false, nil
		}
		s.logError("idempotency lookup failed", "error", err)
		return nil, false, fmt.Errorf("%w: idempotency lookup", ErrStorageUnavailable)
	}
	if existing.ProviderCode != cmd.ProviderCode || existing.ProviderAction != cmd.ProviderAction || !canonicalEqual(existing.Payload, cmd.Payload) {
		return nil, true, ErrSourceRequestConflict
	}
	if s.logger != nil {
		s.logger.DebugContext(ctx, "idempotent duplicate returned without republish",
			"event_id", existing.ID,
			"source_system", existing.SourceSystem,
			"source_request_id", existing.SourceRequestID,
			"status", existing.Status)
	}
	return submitResult(existing, true), true, nil
}

func (s *Service) GetStatus(ctx context.Context, query StatusQuery) (*domain.Event, error) {
	if query.SourceSystem == "" || query.SourceRequestID == "" {
		return nil, ErrInvalidRequest
	}
	event, err := s.repo.FindBySourceRequest(ctx, query.SourceSystem, query.SourceRequestID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, ErrNotFound
		}
		s.logError("get notification failed", "error", err)
		return nil, fmt.Errorf("%w: get notification", ErrStorageUnavailable)
	}
	return event, nil
}

func (s *Service) ListCapabilities(context.Context) []ProviderCapability {
	grouped := make(map[string][]ProviderActionCapability)
	for _, action := range s.registry.All() {
		code := action.Context.ProviderCode
		grouped[code] = append(grouped[code], ProviderActionCapability{
			ProviderAction: action.Context.ProviderAction,
			Description:    action.Description,
		})
	}

	codes := make([]string, 0, len(grouped))
	for code := range grouped {
		codes = append(codes, code)
	}
	sort.Strings(codes)

	capabilities := make([]ProviderCapability, 0, len(codes))
	for _, code := range codes {
		actions := grouped[code]
		sort.Slice(actions, func(i, j int) bool {
			return actions[i].ProviderAction < actions[j].ProviderAction
		})
		capabilities = append(capabilities, ProviderCapability{ProviderCode: code, Actions: actions})
	}
	return capabilities
}

func submitResult(event *domain.Event, duplicate bool) *SubmitResult {
	return &SubmitResult{
		EventID:         event.ID.String(),
		SourceSystem:    event.SourceSystem,
		SourceRequestID: event.SourceRequestID,
		Status:          string(event.Status),
		Duplicate:       duplicate,
		AcceptedAt:      event.CreatedAt,
	}
}

func (s *Service) logError(message string, args ...any) {
	if s.logger != nil {
		s.logger.Error(message, args...)
	}
}
