package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"notification-delivery/internal/domain"
	"notification-delivery/internal/provider"
	"notification-delivery/internal/publish"
	"notification-delivery/internal/store"
)

// EventRepo is the subset of *store.EventRepo the API needs, so handlers
// can be unit tested against a fake.
type EventRepo interface {
	Insert(ctx context.Context, ev *domain.Event) error
	FindBySourceRequest(ctx context.Context, sourceSystem, sourceRequestID string) (*domain.Event, error)
}

type Handlers struct {
	repo      EventRepo
	registry  *provider.Registry
	publisher *publish.Service
	logger    *slog.Logger
}

// createMessage implements DESIGN.md §4.1.
//
// @Summary Submit a notification
// @Description Validates and durably accepts one notification for asynchronous delivery. Reusing source_system and source_request_id with identical content returns the original event as a duplicate.
// @Tags messages
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param Accept-Language header string false "Error message language: zh-CN or en-US"
// @Param request body createLarkBotMessageExample true "Notification to deliver; payload example follows Lark's documented custom-bot text schema"
// @Success 202 {object} createMessageAPIResponse
// @Failure 400 {object} errorResponse
// @Failure 401 {object} errorResponse
// @Failure 409 {object} errorResponse
// @Failure 422 {object} errorResponse
// @Failure 503 {object} errorResponse
// @Router /v1/messages [post]
func (h *Handlers) createMessage(c *gin.Context) {
	var req createMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, errInvalidRequest)
		return
	}
	resolved, ok := h.registry.Lookup(req.ProviderCode, req.ProviderAction)
	if !ok {
		writeError(c, http.StatusUnprocessableEntity, errUnsupportedProviderAction)
		return
	}

	if err := resolved.Adapter.Validate(req.ProviderAction, req.Payload); err != nil {
		var validationErr *provider.ValidationError
		if errors.As(err, &validationErr) {
			writePayloadValidationError(c, validationErr.Problems)
			return
		}
		writeError(c, http.StatusUnprocessableEntity, errInvalidPayload)
		return
	}

	ctx := c.Request.Context()
	sourceSystem := req.SourceSystem

	// resolveIdempotency fully writes the HTTP response itself (202
	// duplicate, 409 conflict, or 503) whenever a prior row exists or the
	// lookup fails; it only returns false when there is genuinely nothing
	// to reconcile against yet.
	if h.resolveIdempotency(c, ctx, sourceSystem, req) {
		return
	}

	ev := &domain.Event{
		ID:              uuid.New(),
		SourceSystem:    sourceSystem,
		SourceRequestID: req.SourceRequestID,
		ProviderCode:    req.ProviderCode,
		ProviderAction:  req.ProviderAction,
		Payload:         req.Payload,
	}
	if err := h.repo.Insert(ctx, ev); err != nil {
		if errors.Is(err, store.ErrDuplicateSourceRequest) {
			// Lost the race against a concurrent identical submission;
			// resolve it exactly like a pre-existing row.
			if h.resolveIdempotency(c, ctx, sourceSystem, req) {
				return
			}
		}
		h.logger.Error("insert notification_event failed", "error", err)
		writeError(c, http.StatusServiceUnavailable, errStorageUnavailable)
		return
	}
	if h.logger != nil {
		h.logger.Debug("notification event persisted",
			"event_id", ev.ID,
			"source_system", ev.SourceSystem,
			"source_request_id", ev.SourceRequestID,
			"provider_code", ev.ProviderCode,
			"provider_action", ev.ProviderAction)
	}

	// Best-effort direct publish. Failure here is not fatal: the
	// compensator picks up rows with enqueued_at IS NULL (DESIGN.md §8).
	if err := h.publisher.PublishAndMark(ctx, ev.ID); err != nil {
		h.logger.Warn("direct publish failed, relying on compensator", "event_id", ev.ID, "error", err)
	} else if h.logger != nil {
		h.logger.Debug("direct publish completed", "event_id", ev.ID)
	}

	writeSuccess(c, http.StatusAccepted, createMessageResponse{
		EventID:         ev.ID.String(),
		SourceSystem:    ev.SourceSystem,
		SourceRequestID: ev.SourceRequestID,
		Status:          string(domain.StatusPending),
		Duplicate:       false,
		AcceptedAt:      ev.CreatedAt,
	})
}

// resolveIdempotency checks for a pre-existing row under (sourceSystem,
// source_request_id). It returns true if it already wrote the complete
// HTTP response — a 202 duplicate, a 409 status=1006, or a 503
// on lookup failure — in which case the caller must not do anything else.
// It returns false only when no row exists yet, meaning the caller should
// proceed to insert one.
func (h *Handlers) resolveIdempotency(c *gin.Context, ctx context.Context, sourceSystem string, req createMessageRequest) bool {
	existing, err := h.repo.FindBySourceRequest(ctx, sourceSystem, req.SourceRequestID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false
		}
		h.logger.Error("idempotency lookup failed", "error", err)
		writeError(c, http.StatusServiceUnavailable, errStorageUnavailable)
		return true
	}

	sameTarget := existing.ProviderCode == req.ProviderCode && existing.ProviderAction == req.ProviderAction
	samePayload := canonicalEqual(existing.Payload, req.Payload)
	if !sameTarget || !samePayload {
		writeError(c, http.StatusConflict, errSourceRequestConflict)
		return true
	}

	writeSuccess(c, http.StatusAccepted, createMessageResponse{
		EventID:         existing.ID.String(),
		SourceSystem:    existing.SourceSystem,
		SourceRequestID: existing.SourceRequestID,
		Status:          string(existing.Status),
		Duplicate:       true,
		AcceptedAt:      existing.CreatedAt,
	})
	if h.logger != nil {
		h.logger.Debug("idempotent duplicate returned without republish",
			"event_id", existing.ID,
			"source_system", existing.SourceSystem,
			"source_request_id", existing.SourceRequestID,
			"status", existing.Status)
	}
	return true
}

// getMessage implements DESIGN.md §4.2.
//
// @Summary Get notification status
// @Description Returns the delivery state for the requested source system and source request ID.
// @Tags messages
// @Produce json
// @Security BearerAuth
// @Param Accept-Language header string false "Error message language: zh-CN or en-US"
// @Param source_request_id path string true "Source request ID"
// @Param source_system query string true "Source system"
// @Success 200 {object} messageStatusAPIResponse
// @Failure 400 {object} errorResponse
// @Failure 401 {object} errorResponse
// @Failure 404 {object} errorResponse
// @Failure 503 {object} errorResponse
// @Router /v1/messages/{source_request_id} [get]
func (h *Handlers) getMessage(c *gin.Context) {
	sourceRequestID := c.Param("source_request_id")
	sourceSystem := c.Query("source_system")
	if sourceSystem == "" {
		writeError(c, http.StatusBadRequest, errInvalidRequest)
		return
	}
	ev, err := h.repo.FindBySourceRequest(c.Request.Context(), sourceSystem, sourceRequestID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(c, http.StatusNotFound, errMessageNotFound)
			return
		}
		h.logger.Error("get message failed", "error", err)
		writeError(c, http.StatusServiceUnavailable, errStorageUnavailable)
		return
	}

	writeSuccess(c, http.StatusOK, messageStatusResponse{
		EventID:          ev.ID.String(),
		SourceSystem:     ev.SourceSystem,
		SourceRequestID:  ev.SourceRequestID,
		ProviderCode:     ev.ProviderCode,
		ProviderAction:   ev.ProviderAction,
		Status:           string(ev.Status),
		AttemptCount:     ev.AttemptCount,
		LastResult:       ev.LastResult,
		ProviderResponse: ev.ProviderResponse,
		CreatedAt:        ev.CreatedAt,
		UpdatedAt:        ev.UpdatedAt,
	})
}
