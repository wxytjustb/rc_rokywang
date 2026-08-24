package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/errorutils"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"

	"notification-delivery/internal/provider"
)

const (
	firebasePushProviderCode = "firebase-push"
	firebasePushActionSend   = "send"
	firebasePushMaxPayload   = 4 * 1024
)

// FirebasePush sends one cross-platform mobile push through Firebase Cloud
// Messaging. FCM routes Android messages directly and iOS messages through
// the Firebase project's APNs configuration. A successful result means only
// that FCM accepted the message and returned a message ID.
type FirebasePush struct {
	projectID           string
	credentialsRequired bool
	initialized         bool
	newClient           firebaseMessagingClientFactory

	clientMu sync.RWMutex
	client   firebaseMessagingClient
}

type firebaseMessagingClient interface {
	Send(context.Context, *messaging.Message) (string, error)
}

type firebaseMessagingClientFactory func(context.Context, string, string) (firebaseMessagingClient, error)

func NewFirebasePush() *FirebasePush {
	return &FirebasePush{newClient: newFirebaseMessagingClient}
}

func (*FirebasePush) ProviderCode() string {
	return firebasePushProviderCode
}

type firebasePushConfig struct {
	ProjectID      string `yaml:"project_id"`
	CredentialsRef string `yaml:"credentials_ref"`
	Actions        struct {
		Send firebasePushSendConfig `yaml:"send"`
	} `yaml:"actions"`
}

type firebasePushSendConfig struct {
	TimeoutMs         int                          `yaml:"timeout_ms"`
	RequestsPerSecond float64                      `yaml:"requests_per_second"`
	MaxConcurrency    int                          `yaml:"max_concurrency"`
	CircuitBreaker    *adapterCircuitBreakerConfig `yaml:"circuit_breaker"`
}

func (a *FirebasePush) Config(raw yaml.Node) (provider.Config, error) {
	var cfg firebasePushConfig
	if err := provider.DecodeConfig(raw, &cfg); err != nil {
		return provider.Config{}, err
	}
	projectID := strings.TrimSpace(cfg.ProjectID)
	if err := validateFirebaseProjectID(projectID); err != nil {
		return provider.Config{}, fmt.Errorf("project_id: %w", err)
	}
	action := cfg.Actions.Send
	if err := validateDeliveryLimits(action.TimeoutMs, action.RequestsPerSecond, action.MaxConcurrency); err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s: %w", firebasePushActionSend, err)
	}
	breaker, err := normalizeCircuitBreaker(action.CircuitBreaker)
	if err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s.circuit_breaker: %w", firebasePushActionSend, err)
	}

	credentialsRef := strings.TrimSpace(cfg.CredentialsRef)
	a.clientMu.Lock()
	a.projectID = projectID
	a.credentialsRequired = credentialsRef != ""
	a.initialized = true
	a.client = nil
	if a.newClient == nil {
		a.newClient = newFirebaseMessagingClient
	}
	a.clientMu.Unlock()

	return provider.Config{
		CredentialRef: credentialsRef,
		Actions: map[string]provider.ActionConfig{
			firebasePushActionSend: {
				Description:       "通过 Firebase Cloud Messaging 向单个 iOS 或 Android 设备发送推送；成功仅表示 FCM 已接受",
				Method:            "FCM",
				TimeoutMs:         action.TimeoutMs,
				RequestsPerSecond: action.RequestsPerSecond,
				MaxConcurrency:    action.MaxConcurrency,
				CircuitBreaker:    breaker,
			},
		},
	}, nil
}

func (*FirebasePush) Validate(action string, payload json.RawMessage) error {
	if action != firebasePushActionSend {
		return fmt.Errorf("firebase_push: unsupported action %q", action)
	}
	if _, err := normalizeFirebasePushPayload(payload); err != nil {
		return &provider.ValidationError{Problems: []string{err.Error()}}
	}
	return nil
}

func (a *FirebasePush) SendActionRequest(ctx context.Context, ac provider.ActionContext, action string, payload json.RawMessage) (*provider.Result, error) {
	if action != firebasePushActionSend {
		return nil, fmt.Errorf("firebase_push: unsupported action %q", action)
	}
	message, err := buildFirebaseMessage(payload)
	if err != nil {
		return nil, fmt.Errorf("build Firebase message: %w", err)
	}
	if !a.initialized {
		return nil, fmt.Errorf("firebase_push: adapter config was not initialized")
	}
	if a.credentialsRequired && strings.TrimSpace(ac.Credential) == "" {
		return &provider.Result{
			ErrorClass: "FCM_CREDENTIAL_EMPTY",
			Message:    "the configured Firebase service-account credential resolved to an empty value",
		}, nil
	}

	timeout := time.Duration(ac.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		return nil, fmt.Errorf("firebase_push: timeout_ms must be greater than zero")
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client, err := a.messagingClient(callCtx, ac.Credential)
	if err != nil {
		return classifyFirebasePushFailure(callCtx, err, "client_init"), nil
	}
	messageID, err := client.Send(callCtx, message)
	if err != nil {
		return classifyFirebasePushFailure(callCtx, err, "send"), nil
	}
	if strings.TrimSpace(messageID) == "" {
		return &provider.Result{
			AvailabilityFailure: true,
			ErrorClass:          "FCM_PROTOCOL_ERROR",
			Message:             "FCM returned success without a message ID",
		}, nil
	}

	providerResponse, _ := json.Marshal(map[string]any{
		"accepted":   true,
		"message_id": messageID,
	})
	return &provider.Result{
		Success:          true,
		Message:          "FCM accepted the message",
		ProviderResponse: providerResponse,
	}, nil
}

func (a *FirebasePush) messagingClient(ctx context.Context, credential string) (firebaseMessagingClient, error) {
	a.clientMu.RLock()
	client := a.client
	a.clientMu.RUnlock()
	if client != nil {
		return client, nil
	}

	a.clientMu.Lock()
	defer a.clientMu.Unlock()
	if a.client != nil {
		return a.client, nil
	}
	client, err := a.newClient(ctx, a.projectID, credential)
	if err != nil {
		return nil, err
	}
	a.client = client
	return client, nil
}

func newFirebaseMessagingClient(ctx context.Context, projectID, credential string) (firebaseMessagingClient, error) {
	clientOptions := make([]option.ClientOption, 0, 1)
	if strings.TrimSpace(credential) != "" {
		var header struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(credential), &header); err != nil {
			return nil, fmt.Errorf("decode Firebase service-account credential: %w", err)
		}
		if header.Type != "service_account" {
			return nil, fmt.Errorf("Firebase credential type must be service_account")
		}
		clientOptions = append(clientOptions, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(credential)))
	}

	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: projectID}, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("initialize Firebase app: %w", err)
	}
	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, fmt.Errorf("initialize Firebase messaging client: %w", err)
	}
	return client, nil
}

type firebasePushPayload struct {
	FID          string                    `json:"fid"`
	Token        string                    `json:"token"`
	Notification *firebaseNotification     `json:"notification"`
	Data         map[string]string         `json:"data"`
	Android      *firebaseAndroidOverrides `json:"android"`
	IOS          *firebaseIOSOverrides     `json:"ios"`
}

type firebaseNotification struct {
	Title    string `json:"title"`
	Body     string `json:"body"`
	ImageURL string `json:"image_url"`
}

type firebaseAndroidOverrides struct {
	Priority  string `json:"priority"`
	ChannelID string `json:"channel_id"`
	Sound     string `json:"sound"`
}

type firebaseIOSOverrides struct {
	Sound            string `json:"sound"`
	Badge            *int   `json:"badge"`
	ContentAvailable bool   `json:"content_available"`
}

func normalizeFirebasePushPayload(payload json.RawMessage) (firebasePushPayload, error) {
	if len(payload) > firebasePushMaxPayload {
		return firebasePushPayload{}, fmt.Errorf("payload is %d bytes; maximum is %d bytes", len(payload), firebasePushMaxPayload)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var value firebasePushPayload
	if err := decoder.Decode(&value); err != nil {
		return firebasePushPayload{}, fmt.Errorf("payload must be a JSON object using only the documented Firebase push fields: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return firebasePushPayload{}, err
	}

	value.FID = strings.TrimSpace(value.FID)
	value.Token = strings.TrimSpace(value.Token)
	if (value.FID == "") == (value.Token == "") {
		return firebasePushPayload{}, fmt.Errorf("payload must specify exactly one non-empty fid or token")
	}
	if value.Notification == nil && len(value.Data) == 0 {
		return firebasePushPayload{}, fmt.Errorf("payload.notification or payload.data must be provided")
	}
	if value.Notification != nil {
		if strings.TrimSpace(value.Notification.Title) == "" && strings.TrimSpace(value.Notification.Body) == "" {
			return firebasePushPayload{}, fmt.Errorf("payload.notification.title or payload.notification.body must be non-empty")
		}
		if value.Notification.ImageURL != "" {
			if err := validateHTTPSURL(value.Notification.ImageURL); err != nil {
				return firebasePushPayload{}, fmt.Errorf("payload.notification.image_url: %w", err)
			}
		}
	}
	for key := range value.Data {
		if strings.TrimSpace(key) == "" {
			return firebasePushPayload{}, fmt.Errorf("payload.data keys must be non-empty")
		}
		lower := strings.ToLower(key)
		if lower == "from" || lower == "message_type" || strings.HasPrefix(lower, "google.") || strings.HasPrefix(lower, "gcm.") {
			return firebasePushPayload{}, fmt.Errorf("payload.data key %q is reserved by FCM", key)
		}
	}
	if value.Android != nil {
		value.Android.Priority = strings.ToLower(strings.TrimSpace(value.Android.Priority))
		if value.Android.Priority != "" && value.Android.Priority != "normal" && value.Android.Priority != "high" {
			return firebasePushPayload{}, fmt.Errorf("payload.android.priority must be normal or high")
		}
		if strings.TrimSpace(value.Android.ChannelID) == "" && value.Android.ChannelID != "" {
			return firebasePushPayload{}, fmt.Errorf("payload.android.channel_id must not contain only whitespace")
		}
		if strings.TrimSpace(value.Android.Sound) == "" && value.Android.Sound != "" {
			return firebasePushPayload{}, fmt.Errorf("payload.android.sound must not contain only whitespace")
		}
	}
	if value.IOS != nil {
		if value.IOS.Badge != nil && *value.IOS.Badge < 0 {
			return firebasePushPayload{}, fmt.Errorf("payload.ios.badge must be zero or greater")
		}
		if strings.TrimSpace(value.IOS.Sound) == "" && value.IOS.Sound != "" {
			return firebasePushPayload{}, fmt.Errorf("payload.ios.sound must not contain only whitespace")
		}
	}
	return value, nil
}

func buildFirebaseMessage(payload json.RawMessage) (*messaging.Message, error) {
	value, err := normalizeFirebasePushPayload(payload)
	if err != nil {
		return nil, err
	}
	message := &messaging.Message{
		Fid:   value.FID,
		Token: value.Token,
		Data:  value.Data,
	}
	if value.Notification != nil {
		message.Notification = &messaging.Notification{
			Title:    value.Notification.Title,
			Body:     value.Notification.Body,
			ImageURL: value.Notification.ImageURL,
		}
	}
	if value.Android != nil {
		message.Android = &messaging.AndroidConfig{Priority: value.Android.Priority}
		if value.Android.ChannelID != "" || value.Android.Sound != "" {
			message.Android.Notification = &messaging.AndroidNotification{
				ChannelID: value.Android.ChannelID,
				Sound:     value.Android.Sound,
			}
		}
	}
	imageURL := ""
	if value.Notification != nil {
		imageURL = value.Notification.ImageURL
	}
	if value.IOS != nil || imageURL != "" {
		aps := &messaging.Aps{}
		message.APNS = &messaging.APNSConfig{
			Payload: &messaging.APNSPayload{Aps: aps},
		}
		if value.IOS != nil {
			aps.Sound = value.IOS.Sound
			aps.Badge = value.IOS.Badge
			aps.ContentAvailable = value.IOS.ContentAvailable
		}
		if imageURL != "" {
			aps.MutableContent = true
			message.APNS.FCMOptions = &messaging.APNSFCMOptions{ImageURL: imageURL}
		}
	}
	return message, nil
}

func classifyFirebasePushFailure(ctx context.Context, err error, phase string) *provider.Result {
	result := &provider.Result{}
	if resp := errorutils.HTTPResponse(err); resp != nil {
		result.HTTPStatus = resp.StatusCode
	}

	switch {
	case errors.Is(context.Cause(ctx), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || errorutils.IsDeadlineExceeded(err):
		result.ErrorClass = "FCM_TIMEOUT"
		result.Message = "Firebase Cloud Messaging request timed out"
		result.AvailabilityFailure = true
	case errors.Is(context.Cause(ctx), context.Canceled) || errors.Is(err, context.Canceled) || errorutils.IsCancelled(err):
		result.ErrorClass = "FCM_CONTEXT_CANCELED"
		result.Message = "Firebase Cloud Messaging request was canceled"
	case messaging.IsUnregistered(err):
		result.ErrorClass = "FCM_TARGET_UNREGISTERED"
		result.Message = "the Firebase installation ID or registration token is no longer registered"
	case messaging.IsThirdPartyAuthError(err):
		result.ErrorClass = "FCM_APNS_AUTH_ERROR"
		result.Message = "Firebase rejected the configured APNs credentials"
	case messaging.IsSenderIDMismatch(err):
		result.ErrorClass = "FCM_SENDER_ID_MISMATCH"
		result.Message = "the target does not belong to the configured Firebase sender"
	case messaging.IsInvalidArgument(err) || errorutils.IsInvalidArgument(err):
		result.ErrorClass = "FCM_INVALID_ARGUMENT"
		result.Message = "Firebase rejected the message as invalid"
	case messaging.IsQuotaExceeded(err) || errorutils.IsResourceExhausted(err):
		result.ErrorClass = "FCM_QUOTA_EXCEEDED"
		result.Message = "Firebase Cloud Messaging quota or rate limit was exceeded"
		result.AvailabilityFailure = true
	case messaging.IsUnavailable(err) || errorutils.IsUnavailable(err):
		result.ErrorClass = "FCM_UNAVAILABLE"
		result.Message = "Firebase Cloud Messaging is temporarily unavailable"
		result.AvailabilityFailure = true
	case messaging.IsInternal(err) || errorutils.IsInternal(err):
		result.ErrorClass = "FCM_INTERNAL_ERROR"
		result.Message = "Firebase Cloud Messaging returned an internal error"
		result.AvailabilityFailure = true
	case errorutils.IsUnauthenticated(err):
		result.ErrorClass = "FCM_UNAUTHENTICATED"
		result.Message = "Firebase rejected the configured credential"
	case errorutils.IsPermissionDenied(err):
		result.ErrorClass = "FCM_PERMISSION_DENIED"
		result.Message = "the configured credential cannot send Firebase Cloud Messaging messages"
	default:
		var networkErr net.Error
		if errors.As(err, &networkErr) {
			result.ErrorClass = "FCM_TRANSPORT_ERROR"
			result.Message = "Firebase Cloud Messaging could not be reached"
			result.AvailabilityFailure = true
		} else if phase == "client_init" {
			result.ErrorClass = "FCM_CLIENT_INIT_ERROR"
			result.Message = "the Firebase messaging client could not be initialized"
		} else {
			result.ErrorClass = "FCM_SEND_ERROR"
			result.Message = "Firebase Cloud Messaging rejected or could not process the message"
		}
	}

	if result.HTTPStatus != 0 {
		result.ProviderResponse, _ = json.Marshal(map[string]any{
			"http_status": result.HTTPStatus,
			"error_class": result.ErrorClass,
		})
	}
	return result
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("payload must contain exactly one JSON value")
		}
		return fmt.Errorf("payload contains trailing invalid JSON: %w", err)
	}
	return nil
}

func validateFirebaseProjectID(projectID string) error {
	if projectID == "" {
		return fmt.Errorf("must not be empty")
	}
	if len(projectID) > 128 {
		return fmt.Errorf("must not exceed 128 characters")
	}
	if strings.ContainsAny(projectID, "/\\?#@ \t\r\n") {
		return fmt.Errorf("must not contain whitespace, URL delimiters, or credentials")
	}
	return nil
}

func validateHTTPSURL(raw string) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("must be an absolute HTTPS URL without credentials or fragment")
	}
	return nil
}
