package publish

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"notification-delivery/internal/mq"
)

type stubPublisher struct {
	result mq.PublishResult
	err    error
}

func (p *stubPublisher) Publish(context.Context, uuid.UUID) (mq.PublishResult, error) {
	return p.result, p.err
}

func (p *stubPublisher) Close() error { return nil }

type stubEnqueueMarker struct {
	calls int
	err   error
}

func (m *stubEnqueueMarker) MarkEnqueued(context.Context, uuid.UUID) error {
	m.calls++
	return m.err
}

func TestPublishAndMarkMarksOnlyDurablePublish(t *testing.T) {
	tests := []struct {
		name      string
		result    mq.PublishResult
		wantCalls int
	}{
		{name: "external broker confirmation", result: mq.PublishResult{Durable: true}, wantCalls: 1},
		{name: "memory wake-up", result: mq.PublishResult{Durable: false}, wantCalls: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marker := &stubEnqueueMarker{}
			service := New(&stubPublisher{result: tt.result}, marker)
			if err := service.PublishAndMark(context.Background(), uuid.New()); err != nil {
				t.Fatalf("PublishAndMark() error = %v", err)
			}
			if marker.calls != tt.wantCalls {
				t.Fatalf("MarkEnqueued calls = %d, want %d", marker.calls, tt.wantCalls)
			}
		})
	}
}

func TestPublishAndMarkDoesNotMarkAfterPublishFailure(t *testing.T) {
	publishErr := errors.New("publish failed")
	marker := &stubEnqueueMarker{}
	service := New(&stubPublisher{err: publishErr}, marker)

	err := service.PublishAndMark(context.Background(), uuid.New())
	if !errors.Is(err, publishErr) {
		t.Fatalf("PublishAndMark() error = %v, want wrapped publish error", err)
	}
	if marker.calls != 0 {
		t.Fatalf("MarkEnqueued calls = %d, want 0", marker.calls)
	}
}

func TestPublishAndMarkReturnsMarkFailure(t *testing.T) {
	markErr := errors.New("mark failed")
	marker := &stubEnqueueMarker{err: markErr}
	service := New(&stubPublisher{result: mq.PublishResult{Durable: true}}, marker)

	err := service.PublishAndMark(context.Background(), uuid.New())
	if !errors.Is(err, markErr) {
		t.Fatalf("PublishAndMark() error = %v, want wrapped mark error", err)
	}
}
