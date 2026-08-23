package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"notification-delivery/internal/application/notification"
)

type Handlers struct {
	service *notification.Service
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

	result, err := h.service.Submit(c.Request.Context(), notification.SubmitCommand{
		SourceSystem:    req.SourceSystem,
		SourceRequestID: req.SourceRequestID,
		ProviderCode:    req.ProviderCode,
		ProviderAction:  req.ProviderAction,
		Payload:         req.Payload,
	})
	if err != nil {
		h.writeSubmitError(c, err)
		return
	}

	writeSuccess(c, http.StatusAccepted, createMessageResponse{
		EventID:         result.EventID,
		SourceSystem:    result.SourceSystem,
		SourceRequestID: result.SourceRequestID,
		Status:          result.Status,
		Duplicate:       result.Duplicate,
		AcceptedAt:      result.AcceptedAt,
	})
}

func (h *Handlers) writeSubmitError(c *gin.Context, err error) {
	var validationErr *notification.PayloadValidationError
	switch {
	case errors.Is(err, notification.ErrInvalidRequest):
		writeError(c, http.StatusBadRequest, errInvalidRequest)
	case errors.Is(err, notification.ErrUnsupportedProviderAction):
		writeError(c, http.StatusUnprocessableEntity, errUnsupportedProviderAction)
	case errors.As(err, &validationErr):
		writePayloadValidationError(c, validationErr.Problems)
	case errors.Is(err, notification.ErrInvalidPayload):
		writeError(c, http.StatusUnprocessableEntity, errInvalidPayload)
	case errors.Is(err, notification.ErrSourceRequestConflict):
		writeError(c, http.StatusConflict, errSourceRequestConflict)
	case errors.Is(err, notification.ErrStorageUnavailable):
		writeError(c, http.StatusServiceUnavailable, errStorageUnavailable)
	default:
		writeError(c, http.StatusInternalServerError, errInternal)
	}
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
	event, err := h.service.GetStatus(c.Request.Context(), notification.StatusQuery{
		SourceSystem:    c.Query("source_system"),
		SourceRequestID: c.Param("source_request_id"),
	})
	if err != nil {
		switch {
		case errors.Is(err, notification.ErrInvalidRequest):
			writeError(c, http.StatusBadRequest, errInvalidRequest)
		case errors.Is(err, notification.ErrNotFound):
			writeError(c, http.StatusNotFound, errMessageNotFound)
		case errors.Is(err, notification.ErrStorageUnavailable):
			writeError(c, http.StatusServiceUnavailable, errStorageUnavailable)
		default:
			writeError(c, http.StatusInternalServerError, errInternal)
		}
		return
	}

	writeSuccess(c, http.StatusOK, messageStatusResponse{
		EventID:          event.ID.String(),
		SourceSystem:     event.SourceSystem,
		SourceRequestID:  event.SourceRequestID,
		ProviderCode:     event.ProviderCode,
		ProviderAction:   event.ProviderAction,
		Status:           string(event.Status),
		AttemptCount:     event.AttemptCount,
		LastResult:       event.LastResult,
		ProviderResponse: event.ProviderResponse,
		CreatedAt:        event.CreatedAt,
		UpdatedAt:        event.UpdatedAt,
	})
}
