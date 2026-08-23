package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"notification-delivery/internal/application/notification"
	"notification-delivery/internal/authn"
	"notification-delivery/internal/domain"
)

type recordingEventRepo struct {
	found           *domain.Event
	sourceSystem    string
	sourceRequestID string
}

func (r *recordingEventRepo) Insert(context.Context, *domain.Event) error {
	return nil
}

func (r *recordingEventRepo) FindBySourceRequest(_ context.Context, sourceSystem, sourceRequestID string) (*domain.Event, error) {
	r.sourceSystem = sourceSystem
	r.sourceRequestID = sourceRequestID
	return r.found, nil
}

func TestGetMessageUsesExplicitSourceSystemQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &recordingEventRepo{found: &domain.Event{
		ID:              uuid.New(),
		SourceSystem:    "billing-system",
		SourceRequestID: "request-1",
		Status:          domain.StatusPending,
	}}
	router := NewRouter(Deps{
		Service:      notification.NewService(repo, nil, nil, nil),
		AuthVerifier: authn.NewVerifier([]string{"shared-token"}),
	})

	request := httptest.NewRequest(http.MethodGet, "/v1/messages/request-1?source_system=billing-system", nil)
	request.Header.Set("Authorization", "Bearer shared-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("GET message status = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body.String())
	}
	if repo.sourceSystem != "billing-system" || repo.sourceRequestID != "request-1" {
		t.Fatalf("repository key = (%q, %q), want (billing-system, request-1)", repo.sourceSystem, repo.sourceRequestID)
	}
}

func TestGetMessageRequiresSourceSystemQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &recordingEventRepo{}
	router := NewRouter(Deps{
		Service:      notification.NewService(repo, nil, nil, nil),
		AuthVerifier: authn.NewVerifier([]string{"shared-token"}),
	})

	request := httptest.NewRequest(http.MethodGet, "/v1/messages/request-1", nil)
	request.Header.Set("Authorization", "Bearer shared-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("GET message status = %d, want %d; body = %s", response.Code, http.StatusBadRequest, response.Body.String())
	}
	if repo.sourceSystem != "" || repo.sourceRequestID != "" {
		t.Fatal("repository was called without the required source_system query")
	}
}
