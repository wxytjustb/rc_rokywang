package grpcapi

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	notificationv1 "notification-delivery/gen/notification/v1"
	"notification-delivery/internal/application/notification"
	"notification-delivery/internal/domain"
)

type notificationServer struct {
	notificationv1.UnimplementedNotificationServiceServer
	service *notification.Service
}

func (s *notificationServer) SubmitNotification(ctx context.Context, req *notificationv1.SubmitNotificationRequest) (*notificationv1.SubmitNotificationResponse, error) {
	result, err := s.service.Submit(ctx, notification.SubmitCommand{
		SourceSystem:    req.GetSourceSystem(),
		SourceRequestID: req.GetSourceRequestId(),
		ProviderCode:    req.GetProviderCode(),
		ProviderAction:  req.GetProviderAction(),
		Payload:         req.GetPayloadJson(),
	})
	if err != nil {
		return nil, applicationError(err)
	}
	return &notificationv1.SubmitNotificationResponse{
		EventId:         result.EventID,
		SourceSystem:    result.SourceSystem,
		SourceRequestId: result.SourceRequestID,
		Status:          protoStatus(result.Status),
		Duplicate:       result.Duplicate,
		AcceptedAt:      timestamppb.New(result.AcceptedAt),
	}, nil
}

func (s *notificationServer) GetNotificationStatus(ctx context.Context, req *notificationv1.GetNotificationStatusRequest) (*notificationv1.NotificationStatusResponse, error) {
	event, err := s.service.GetStatus(ctx, notification.StatusQuery{
		SourceSystem:    req.GetSourceSystem(),
		SourceRequestID: req.GetSourceRequestId(),
	})
	if err != nil {
		return nil, applicationError(err)
	}
	return &notificationv1.NotificationStatusResponse{
		EventId:              event.ID.String(),
		SourceSystem:         event.SourceSystem,
		SourceRequestId:      event.SourceRequestID,
		ProviderCode:         event.ProviderCode,
		ProviderAction:       event.ProviderAction,
		Status:               protoStatus(string(event.Status)),
		AttemptCount:         int32(event.AttemptCount),
		LastResultJson:       append([]byte(nil), event.LastResult...),
		ProviderResponseJson: append([]byte(nil), event.ProviderResponse...),
		CreatedAt:            timestamppb.New(event.CreatedAt),
		UpdatedAt:            timestamppb.New(event.UpdatedAt),
	}, nil
}

func (s *notificationServer) ListProviderCapabilities(ctx context.Context, _ *notificationv1.ListProviderCapabilitiesRequest) (*notificationv1.ListProviderCapabilitiesResponse, error) {
	capabilities := s.service.ListCapabilities(ctx)
	providers := make([]*notificationv1.ProviderCapability, 0, len(capabilities))
	for _, capability := range capabilities {
		actions := make([]*notificationv1.ProviderActionCapability, 0, len(capability.Actions))
		for _, action := range capability.Actions {
			actions = append(actions, &notificationv1.ProviderActionCapability{
				ProviderAction: action.ProviderAction,
				Description:    action.Description,
			})
		}
		providers = append(providers, &notificationv1.ProviderCapability{
			ProviderCode: capability.ProviderCode,
			Actions:      actions,
		})
	}
	return &notificationv1.ListProviderCapabilitiesResponse{Providers: providers}, nil
}

func applicationError(err error) error {
	var validationErr *notification.PayloadValidationError
	switch {
	case errors.Is(err, notification.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, "invalid notification request")
	case errors.Is(err, notification.ErrUnsupportedProviderAction):
		return status.Error(codes.InvalidArgument, "unsupported provider action")
	case errors.As(err, &validationErr):
		message := "invalid provider payload"
		if len(validationErr.Problems) > 0 {
			message += ": " + strings.Join(validationErr.Problems, "; ")
		}
		return status.Error(codes.InvalidArgument, message)
	case errors.Is(err, notification.ErrInvalidPayload):
		return status.Error(codes.InvalidArgument, "invalid provider payload")
	case errors.Is(err, notification.ErrSourceRequestConflict):
		return status.Error(codes.AlreadyExists, "source request conflicts with an existing notification")
	case errors.Is(err, notification.ErrNotFound):
		return status.Error(codes.NotFound, "notification not found")
	case errors.Is(err, notification.ErrStorageUnavailable):
		return status.Error(codes.Unavailable, "notification storage unavailable")
	default:
		return status.Error(codes.Internal, "internal server error")
	}
}

func protoStatus(value string) notificationv1.NotificationStatus {
	switch domain.Status(value) {
	case domain.StatusPending:
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_PENDING
	case domain.StatusProcessing:
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_PROCESSING
	case domain.StatusFailed:
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_FAILED
	}
	switch value {
	case "SUCCEEDED":
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_SUCCEEDED
	case "UNKNOWN":
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_UNKNOWN
	default:
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_UNSPECIFIED
	}
}
