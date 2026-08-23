package adapters

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"notification-delivery/internal/provider"
)

const (
	smtpEmailProviderCode = "smtp-email"
	smtpActionSend        = "send"
	smtpMaxPayloadBytes   = 256 * 1024
	smtpMaxRecipients     = 100

	smtpTLSStartTLS = "starttls"
	smtpTLSImplicit = "implicit_tls"
	smtpTLSDisabled = "disabled"
)

// SMTPEmail sends a single transactional email. A successful result means
// only that the configured SMTP server accepted the message after DATA; it
// does not claim inbox delivery, reading, or freedom from later bounce.
//
// The adapter keeps its validated, non-secret SMTP settings private. The
// password is resolved separately through provider.CredentialResolver and is
// supplied to SendActionRequest through ActionContext.Credential.
type SMTPEmail struct {
	send smtpSendRuntimeConfig
}

func NewSMTPEmail() *SMTPEmail {
	return &SMTPEmail{}
}

func (*SMTPEmail) ProviderCode() string {
	return smtpEmailProviderCode
}

type smtpEmailConfig struct {
	PasswordRef string `yaml:"password_ref"`
	Actions     struct {
		Send smtpSendConfig `yaml:"send"`
	} `yaml:"actions"`
}

type smtpSendConfig struct {
	Host              string                       `yaml:"host"`
	Port              int                          `yaml:"port"`
	TLSMode           string                       `yaml:"tls_mode"`
	Username          string                       `yaml:"username"`
	FromAddress       string                       `yaml:"from_address"`
	FromName          string                       `yaml:"from_name"`
	TimeoutMs         int                          `yaml:"timeout_ms"`
	RequestsPerSecond float64                      `yaml:"requests_per_second"`
	MaxConcurrency    int                          `yaml:"max_concurrency"`
	CircuitBreaker    *adapterCircuitBreakerConfig `yaml:"circuit_breaker"`
}

type smtpSendRuntimeConfig struct {
	Host        string
	Address     string
	TLSMode     string
	Username    string
	From        mail.Address
	Initialized bool
}

func (a *SMTPEmail) Config(raw yaml.Node) (provider.Config, error) {
	var cfg smtpEmailConfig
	if err := provider.DecodeConfig(raw, &cfg); err != nil {
		return provider.Config{}, err
	}
	action := cfg.Actions.Send
	if err := validateDeliveryLimits(action.TimeoutMs, action.RequestsPerSecond, action.MaxConcurrency); err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s: %w", smtpActionSend, err)
	}

	host := strings.TrimSpace(action.Host)
	if err := validateSMTPHost(host); err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s.host: %w", smtpActionSend, err)
	}
	if action.Port < 1 || action.Port > 65535 {
		return provider.Config{}, fmt.Errorf("actions.%s.port must be between 1 and 65535", smtpActionSend)
	}
	tlsMode := strings.ToLower(strings.TrimSpace(action.TLSMode))
	if tlsMode == "" {
		tlsMode = smtpTLSStartTLS
	}
	switch tlsMode {
	case smtpTLSStartTLS, smtpTLSImplicit:
	case smtpTLSDisabled:
		if !isLoopbackHost(host) {
			return provider.Config{}, fmt.Errorf("actions.%s.tls_mode disabled is allowed only for a loopback test server", smtpActionSend)
		}
	default:
		return provider.Config{}, fmt.Errorf("actions.%s.tls_mode must be starttls, implicit_tls, or disabled", smtpActionSend)
	}

	username := strings.TrimSpace(action.Username)
	passwordRef := strings.TrimSpace(cfg.PasswordRef)
	if (username == "") != (passwordRef == "") {
		return provider.Config{}, fmt.Errorf("username and password_ref must either both be configured or both be empty")
	}
	from, err := parseSingleAddress(action.FromAddress, "from_address")
	if err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s.%w", smtpActionSend, err)
	}
	from.Name = strings.TrimSpace(action.FromName)

	breaker, err := normalizeCircuitBreaker(action.CircuitBreaker)
	if err != nil {
		return provider.Config{}, fmt.Errorf("actions.%s.circuit_breaker: %w", smtpActionSend, err)
	}

	a.send = smtpSendRuntimeConfig{
		Host:        host,
		Address:     net.JoinHostPort(host, strconv.Itoa(action.Port)),
		TLSMode:     tlsMode,
		Username:    username,
		From:        *from,
		Initialized: true,
	}
	return provider.Config{
		CredentialRef: passwordRef,
		Actions: map[string]provider.ActionConfig{
			smtpActionSend: {
				Description:       "通过 SMTP 发送纯文本或 HTML 邮件；成功仅表示 SMTP 服务器已接受，不代表最终送达",
				Method:            "SMTP",
				TimeoutMs:         action.TimeoutMs,
				RequestsPerSecond: action.RequestsPerSecond,
				MaxConcurrency:    action.MaxConcurrency,
				CircuitBreaker:    breaker,
			},
		},
	}, nil
}

func (*SMTPEmail) Validate(action string, payload json.RawMessage) error {
	if action != smtpActionSend {
		return fmt.Errorf("smtp_email: unsupported action %q", action)
	}
	_, err := normalizeSMTPPayload(payload)
	if err != nil {
		return &provider.ValidationError{Problems: []string{err.Error()}}
	}
	return nil
}

func (a *SMTPEmail) SendActionRequest(ctx context.Context, ac provider.ActionContext, action string, payload json.RawMessage) (*provider.Result, error) {
	if action != smtpActionSend {
		return nil, fmt.Errorf("smtp_email: unsupported action %q", action)
	}
	if !a.send.Initialized {
		return nil, fmt.Errorf("smtp_email: adapter config was not initialized")
	}
	normalized, err := normalizeSMTPPayload(payload)
	if err != nil {
		return nil, fmt.Errorf("normalize SMTP payload: %w", err)
	}
	message, err := buildSMTPMessage(payload, a.send.From, ac.SourceSystem, ac.SourceRequestID, time.Now())
	if err != nil {
		return nil, fmt.Errorf("build SMTP message: %w", err)
	}
	if a.send.Username != "" && ac.Credential == "" {
		return nil, fmt.Errorf("smtp_email: resolved SMTP password is empty")
	}

	recipients := make([]string, 0, len(normalized.Recipients))
	for _, recipient := range normalized.Recipients {
		recipients = append(recipients, recipient.Address)
	}
	stage, err := sendSMTP(ctx, ac.TimeoutMs, a.send, ac.Credential, recipients, message)
	if err != nil {
		return classifySMTPFailure(ctx, stage, err), nil
	}
	providerResponse, _ := json.Marshal(map[string]any{
		"accepted": true,
		"stage":    "smtp_data_accepted",
	})
	return &provider.Result{
		Success:             true,
		AvailabilityFailure: false,
		Message:             "SMTP server accepted the message",
		ProviderResponse:    providerResponse,
	}, nil
}

type smtpPayload struct {
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
	HTML    string   `json:"html"`
}

type normalizedSMTPPayload struct {
	Recipients []*mail.Address
	Subject    string
	Text       string
	HTML       string
}

func normalizeSMTPPayload(payload json.RawMessage) (normalizedSMTPPayload, error) {
	if len(payload) > smtpMaxPayloadBytes {
		return normalizedSMTPPayload{}, fmt.Errorf("payload is %d bytes; maximum is %d bytes", len(payload), smtpMaxPayloadBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var value smtpPayload
	if err := decoder.Decode(&value); err != nil {
		return normalizedSMTPPayload{}, fmt.Errorf("payload must be a JSON object with only to, subject, text, and html: %w", err)
	}
	if len(value.To) == 0 {
		return normalizedSMTPPayload{}, fmt.Errorf("payload.to must contain at least one recipient")
	}
	if len(value.To) > smtpMaxRecipients {
		return normalizedSMTPPayload{}, fmt.Errorf("payload.to contains %d recipients; maximum is %d", len(value.To), smtpMaxRecipients)
	}
	if strings.TrimSpace(value.Subject) == "" {
		return normalizedSMTPPayload{}, fmt.Errorf("payload.subject must be a non-empty string")
	}
	if strings.ContainsAny(value.Subject, "\r\n") {
		return normalizedSMTPPayload{}, fmt.Errorf("payload.subject must not contain CR or LF")
	}
	if value.Text == "" && value.HTML == "" {
		return normalizedSMTPPayload{}, fmt.Errorf("payload.text or payload.html must be non-empty")
	}

	recipients := make([]*mail.Address, 0, len(value.To))
	for i, raw := range value.To {
		address, err := parseSingleAddress(raw, fmt.Sprintf("to[%d]", i))
		if err != nil {
			return normalizedSMTPPayload{}, err
		}
		recipients = append(recipients, address)
	}
	return normalizedSMTPPayload{
		Recipients: recipients,
		Subject:    value.Subject,
		Text:       value.Text,
		HTML:       value.HTML,
	}, nil
}

func buildSMTPMessage(payload json.RawMessage, from mail.Address, sourceSystem, sourceRequestID string, now time.Time) ([]byte, error) {
	message, err := normalizeSMTPPayload(payload)
	if err != nil {
		return nil, err
	}

	toHeader := make([]string, 0, len(message.Recipients))
	for _, recipient := range message.Recipients {
		toHeader = append(toHeader, recipient.String())
	}
	messageID := stableMessageID(sourceSystem, sourceRequestID, from.Address, payload)

	var out bytes.Buffer
	writeHeader := func(name, value string) {
		fmt.Fprintf(&out, "%s: %s\r\n", name, value)
	}
	writeHeader("Date", now.UTC().Format(time.RFC1123Z))
	writeHeader("From", from.String())
	writeHeader("To", strings.Join(toHeader, ", "))
	writeHeader("Subject", mime.QEncoding.Encode("UTF-8", message.Subject))
	writeHeader("Message-ID", messageID)
	writeHeader("MIME-Version", "1.0")

	if message.Text != "" && message.HTML != "" {
		writer := multipart.NewWriter(&out)
		writeHeader("Content-Type", fmt.Sprintf("multipart/alternative; boundary=%q", writer.Boundary()))
		out.WriteString("\r\n")
		if err := writeMIMEPart(writer, "text/plain; charset=UTF-8", message.Text); err != nil {
			return nil, err
		}
		if err := writeMIMEPart(writer, "text/html; charset=UTF-8", message.HTML); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	}

	contentType := "text/plain; charset=UTF-8"
	body := message.Text
	if body == "" {
		contentType = "text/html; charset=UTF-8"
		body = message.HTML
	}
	writeHeader("Content-Type", contentType)
	writeHeader("Content-Transfer-Encoding", "quoted-printable")
	out.WriteString("\r\n")
	quoted := quotedprintable.NewWriter(&out)
	if _, err := quoted.Write([]byte(body)); err != nil {
		return nil, err
	}
	if err := quoted.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeMIMEPart(writer *multipart.Writer, contentType, body string) error {
	header := make(textproto.MIMEHeader)
	header.Set("Content-Type", contentType)
	header.Set("Content-Transfer-Encoding", "quoted-printable")
	part, err := writer.CreatePart(header)
	if err != nil {
		return err
	}
	quoted := quotedprintable.NewWriter(part)
	if _, err := quoted.Write([]byte(body)); err != nil {
		return err
	}
	return quoted.Close()
}

func stableMessageID(sourceSystem, sourceRequestID, fromAddress string, payload []byte) string {
	seed := sourceSystem + "\x00" + sourceRequestID
	if sourceSystem == "" && sourceRequestID == "" {
		seed = string(payload)
	}
	sum := sha256.Sum256([]byte(seed))
	domain := "notification-delivery.local"
	if at := strings.LastIndex(fromAddress, "@"); at >= 0 && at+1 < len(fromAddress) {
		domain = fromAddress[at+1:]
	}
	return "<" + hex.EncodeToString(sum[:16]) + "@" + domain + ">"
}

func sendSMTP(ctx context.Context, timeoutMs int, cfg smtpSendRuntimeConfig, password string, recipients []string, message []byte) (string, error) {
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stage := "connect"
	conn, err := dialSMTP(callCtx, cfg)
	if err != nil {
		return stage, err
	}
	defer conn.Close()
	if deadline, ok := callCtx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return "connection_deadline", err
		}
	}
	stopClose := context.AfterFunc(callCtx, func() { _ = conn.Close() })
	defer stopClose()

	stage = "greeting"
	client, err := smtp.NewClient(conn, cfg.Host)
	if err != nil {
		return stage, err
	}
	defer client.Close()

	if cfg.TLSMode == smtpTLSStartTLS {
		stage = "starttls"
		ok, _ := client.Extension("STARTTLS")
		if !ok {
			return stage, fmt.Errorf("SMTP server does not advertise STARTTLS")
		}
		if err := client.StartTLS(&tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}); err != nil {
			return stage, err
		}
	}
	if cfg.Username != "" {
		stage = "auth"
		if err := client.Auth(smtp.PlainAuth("", cfg.Username, password, cfg.Host)); err != nil {
			return stage, err
		}
	}

	stage = "mail_from"
	if err := client.Mail(cfg.From.Address); err != nil {
		return stage, err
	}
	for _, recipient := range recipients {
		stage = "recipient"
		if err := client.Rcpt(recipient); err != nil {
			return stage, err
		}
	}

	stage = "data_open"
	data, err := client.Data()
	if err != nil {
		return stage, err
	}
	stage = "data_write"
	if _, err := data.Write(message); err != nil {
		_ = data.Close()
		return stage, err
	}
	stage = "data_commit"
	if err := data.Close(); err != nil {
		return stage, err
	}
	// DATA was explicitly accepted. A subsequent QUIT failure does not make
	// the delivery unknown and must not trigger a duplicate retry.
	_ = client.Quit()
	return "accepted", nil
}

func dialSMTP(ctx context.Context, cfg smtpSendRuntimeConfig) (net.Conn, error) {
	dialer := &net.Dialer{}
	if cfg.TLSMode == smtpTLSImplicit {
		tlsDialer := &tls.Dialer{
			NetDialer: dialer,
			Config:    &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12},
		}
		return tlsDialer.DialContext(ctx, "tcp", cfg.Address)
	}
	return dialer.DialContext(ctx, "tcp", cfg.Address)
}

func classifySMTPFailure(ctx context.Context, stage string, err error) *provider.Result {
	errorClass := "SMTP_PROTOCOL_ERROR"
	availabilityFailure := true
	message := err.Error()

	if cause := context.Cause(ctx); cause != nil {
		errorClass = "SMTP_CONTEXT_CANCELED"
		availabilityFailure = false
		message = cause.Error()
	} else {
		var protocolErr *textproto.Error
		var networkErr net.Error
		switch {
		case errors.As(err, &protocolErr):
			if protocolErr.Code >= 400 && protocolErr.Code < 500 {
				errorClass = "SMTP_TEMPORARY_REJECTION"
				availabilityFailure = true
			} else {
				errorClass = "SMTP_PERMANENT_REJECTION"
				availabilityFailure = false
			}
		case errors.As(err, &networkErr) && networkErr.Timeout():
			errorClass = "SMTP_TIMEOUT"
		case stage == "connect" || stage == "greeting" || stage == "starttls":
			errorClass = "SMTP_CONNECTION_FAILURE"
		case stage == "data_write" || stage == "data_commit":
			errorClass = "SMTP_ACCEPTANCE_UNCONFIRMED"
		}
	}
	return &provider.Result{
		AvailabilityFailure: availabilityFailure,
		ErrorClass:          errorClass,
		Message:             message,
	}
}

func validateSMTPHost(host string) error {
	if host == "" {
		return fmt.Errorf("must not be empty")
	}
	if strings.ContainsAny(host, "/@?# \t\r\n") {
		return fmt.Errorf("must be a hostname or IP address without scheme, path, credentials, or whitespace")
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func parseSingleAddress(raw, field string) (*mail.Address, error) {
	if strings.ContainsAny(raw, "\r\n") {
		return nil, fmt.Errorf("%s must not contain CR or LF", field)
	}
	address, err := mail.ParseAddress(strings.TrimSpace(raw))
	if err != nil || strings.TrimSpace(address.Address) == "" {
		return nil, fmt.Errorf("%s must be one valid email address", field)
	}
	return address, nil
}
