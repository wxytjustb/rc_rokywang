package notification

import "errors"

var (
	ErrInvalidRequest            = errors.New("invalid notification request")
	ErrUnsupportedProviderAction = errors.New("unsupported provider action")
	ErrInvalidPayload            = errors.New("invalid provider payload")
	ErrSourceRequestConflict     = errors.New("source request conflicts with an existing notification")
	ErrNotFound                  = errors.New("notification not found")
	ErrStorageUnavailable        = errors.New("notification storage unavailable")
)

// PayloadValidationError contains safe provider-validation details that may
// be returned through each public protocol.
type PayloadValidationError struct {
	Problems []string
}

func (e *PayloadValidationError) Error() string { return ErrInvalidPayload.Error() }
func (e *PayloadValidationError) Unwrap() error { return ErrInvalidPayload }
