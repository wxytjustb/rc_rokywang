// Package publish is the one place that hands an event_id to the selected
// delivery backend and records enqueued_at after a durable broker receipt.
// The server's direct publish and the compensator therefore share exactly
// the same durability semantics.
package publish

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"notification-delivery/internal/mq"
)

type Service struct {
	broker mq.Publisher
	repo   EnqueueMarker
}

// EnqueueMarker is the database transition needed after a durable broker
// acknowledgement. Keeping the boundary narrow also makes durability
// semantics independently testable.
type EnqueueMarker interface {
	MarkEnqueued(ctx context.Context, id uuid.UUID) error
}

func New(broker mq.Publisher, repo EnqueueMarker) *Service {
	return &Service{broker: broker, repo: repo}
}

// PublishAndMark publishes eventID and sets enqueued_at only for a durable
// broker receipt. A memory wake-up deliberately leaves enqueued_at NULL so
// PostgreSQL can recover it after process loss. If a durable publish succeeds
// but the database update fails, the next scan republishes the event; this is
// the accepted at-least-once behavior described in DESIGN.md §8.
func (s *Service) PublishAndMark(ctx context.Context, id uuid.UUID) error {
	result, err := s.broker.Publish(ctx, id)
	if err != nil {
		return fmt.Errorf("publish %s: %w", id, err)
	}
	if !result.Durable {
		return nil
	}
	if err := s.repo.MarkEnqueued(ctx, id); err != nil {
		return fmt.Errorf("mark enqueued %s: %w", id, err)
	}
	return nil
}
