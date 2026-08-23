package adapters

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/mail"
	"net/textproto"
	"strconv"
	"strings"
	"testing"
	"time"

	"notification-delivery/internal/provider"
)

func TestSMTPEmailConfigNormalizesAction(t *testing.T) {
	adapter := NewSMTPEmail()
	cfg, err := adapter.Config(configNode(t, `
password_ref: vault://notification/smtp-password
actions:
  send:
    host: smtp.example.com
    port: 587
    tls_mode: starttls
    username: notifier@example.com
    from_address: notifier@example.com
    from_name: Notification Delivery
    timeout_ms: 10000
    requests_per_second: 10
    max_concurrency: 4
    circuit_breaker:
      failure_threshold: 5
      open_duration: 30s
`))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	action := cfg.Actions[smtpActionSend]
	if cfg.CredentialRef != "vault://notification/smtp-password" {
		t.Fatalf("credential ref = %q", cfg.CredentialRef)
	}
	if action.Method != "SMTP" || action.TimeoutMs != 10000 || action.MaxConcurrency != 4 {
		t.Fatalf("normalized action = %+v", action)
	}
	if action.CircuitBreaker == nil || action.CircuitBreaker.OpenDuration != 30*time.Second {
		t.Fatalf("normalized circuit breaker = %+v", action.CircuitBreaker)
	}
	if !adapter.send.Initialized || adapter.send.Address != "smtp.example.com:587" {
		t.Fatalf("runtime config = %+v", adapter.send)
	}
}

func TestSMTPEmailConfigRejectsUnsafeOrIncompleteConfig(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "plaintext remote",
			body: `actions:
  send:
    host: smtp.example.com
    port: 25
    tls_mode: disabled
    from_address: notifier@example.com
    timeout_ms: 1000
    requests_per_second: 1
    max_concurrency: 1`,
			want: "loopback",
		},
		{
			name: "username without password ref",
			body: `actions:
  send:
    host: smtp.example.com
    port: 587
    username: notifier@example.com
    from_address: notifier@example.com
    timeout_ms: 1000
    requests_per_second: 1
    max_concurrency: 1`,
			want: "username and password_ref",
		},
		{
			name: "unknown field",
			body: `actions:
  send:
    host: smtp.example.com
    port: 587
    from_address: notifier@example.com
    timeout_ms: 1000
    requests_per_second: 1
    max_concurrency: 1
    endpoint_url: https://example.com`,
			want: "field endpoint_url not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSMTPEmail().Config(configNode(t, tt.body))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Config() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestSMTPEmailValidate(t *testing.T) {
	adapter := NewSMTPEmail()
	for _, payload := range []string{
		`{"to":["user@example.com"],"subject":"text","text":"hello"}`,
		`{"to":["User <user@example.com>"],"subject":"html","html":"<p>hello</p>"}`,
		`{"to":["a@example.com","b@example.com"],"subject":"both","text":"hello","html":"<p>hello</p>"}`,
	} {
		if err := adapter.Validate(smtpActionSend, json.RawMessage(payload)); err != nil {
			t.Errorf("Validate(%s) error = %v", payload, err)
		}
	}

	for _, payload := range []string{
		`{}`,
		`{"to":["not-an-address"],"subject":"bad","text":"hello"}`,
		`{"to":["user@example.com"],"subject":"bad\r\nBcc: victim@example.com","text":"hello"}`,
		`{"to":["user@example.com"],"subject":"missing body"}`,
		`{"to":["user@example.com"],"subject":"unknown","text":"hello","cc":["other@example.com"]}`,
	} {
		if err := adapter.Validate(smtpActionSend, json.RawMessage(payload)); err == nil {
			t.Errorf("Validate(%s) error = nil", payload)
		}
	}
}

func TestBuildSMTPMessageUsesStableMessageIDAndMultipartAlternative(t *testing.T) {
	payload := json.RawMessage(`{"to":["User <user@example.com>"],"subject":"状态更新","text":"plain","html":"<p>html</p>"}`)
	from := mail.Address{Name: "Notifier", Address: "notifier@example.com"}
	first, err := buildSMTPMessage(payload, from, "source-a", "request-1", time.Unix(100, 0))
	if err != nil {
		t.Fatalf("buildSMTPMessage() error = %v", err)
	}
	second, err := buildSMTPMessage(payload, from, "source-a", "request-1", time.Unix(200, 0))
	if err != nil {
		t.Fatalf("buildSMTPMessage() second error = %v", err)
	}
	firstID := messageHeaderValue(string(first), "Message-ID")
	secondID := messageHeaderValue(string(second), "Message-ID")
	if firstID == "" || firstID != secondID {
		t.Fatalf("Message-ID values = %q and %q, want equal non-empty IDs", firstID, secondID)
	}
	third, err := buildSMTPMessage(payload, from, "source-b", "request-1", time.Unix(200, 0))
	if err != nil {
		t.Fatalf("buildSMTPMessage() third error = %v", err)
	}
	if thirdID := messageHeaderValue(string(third), "Message-ID"); thirdID == firstID {
		t.Fatalf("different source systems produced the same Message-ID %q", thirdID)
	}
	if !strings.Contains(string(first), "multipart/alternative") || !strings.Contains(string(first), "text/plain") || !strings.Contains(string(first), "text/html") {
		t.Fatalf("message is not multipart alternative:\n%s", first)
	}
}

func TestSMTPEmailSendSucceedsOnlyAfterDATAAccepted(t *testing.T) {
	address, received, serverErr := startFakeSMTPServer(t)
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("SplitHostPort() error = %v", err)
	}
	port, _ := strconv.Atoi(portText)

	adapter := NewSMTPEmail()
	_, err = adapter.Config(configNode(t, fmt.Sprintf(`
actions:
  send:
    host: %s
    port: %d
    tls_mode: disabled
    from_address: notifier@example.com
    from_name: Notifier
    timeout_ms: 3000
    requests_per_second: 10
    max_concurrency: 2
`, host, port)))
	if err != nil {
		t.Fatalf("Config() error = %v", err)
	}

	result, err := adapter.SendActionRequest(context.Background(), provider.ActionContext{
		ProviderCode:    smtpEmailProviderCode,
		ProviderAction:  smtpActionSend,
		SourceSystem:    "source-a",
		SourceRequestID: "request-1",
		TimeoutMs:       3000,
	}, smtpActionSend, json.RawMessage(`{"to":["user@example.com"],"subject":"test","text":"hello"}`))
	if err != nil {
		t.Fatalf("SendActionRequest() error = %v", err)
	}
	if result == nil || !result.Success || result.AvailabilityFailure {
		t.Fatalf("result = %+v", result)
	}
	select {
	case message := <-received:
		if !strings.Contains(message, "Subject: test") || !strings.Contains(message, "hello") {
			t.Fatalf("received message = %q", message)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SMTP message")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("fake SMTP server error = %v", err)
	}
}

func TestClassifySMTPFailureUsesReplyClass(t *testing.T) {
	temporary := classifySMTPFailure(context.Background(), "recipient", &textproto.Error{Code: 450, Msg: "try later"})
	if temporary.ErrorClass != "SMTP_TEMPORARY_REJECTION" || !temporary.AvailabilityFailure {
		t.Fatalf("temporary result = %+v", temporary)
	}
	permanent := classifySMTPFailure(context.Background(), "recipient", &textproto.Error{Code: 550, Msg: "rejected"})
	if permanent.ErrorClass != "SMTP_PERMANENT_REJECTION" || permanent.AvailabilityFailure {
		t.Fatalf("permanent result = %+v", permanent)
	}
}

func startFakeSMTPServer(t *testing.T) (string, <-chan string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake SMTP server: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	received := make(chan string, 1)
	errors := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errors <- err
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		write := func(line string) error {
			if _, err := writer.WriteString(line); err != nil {
				return err
			}
			return writer.Flush()
		}
		if err := write("220 localhost ESMTP ready\r\n"); err != nil {
			errors <- err
			return
		}
		var message strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				errors <- err
				return
			}
			command := strings.TrimSpace(line)
			switch {
			case strings.HasPrefix(command, "EHLO"):
				err = write("250-localhost\r\n250 PIPELINING\r\n")
			case strings.HasPrefix(command, "MAIL FROM:"):
				err = write("250 sender accepted\r\n")
			case strings.HasPrefix(command, "RCPT TO:"):
				err = write("250 recipient accepted\r\n")
			case command == "DATA":
				if err = write("354 end with <CRLF>.<CRLF>\r\n"); err == nil {
					for {
						dataLine, readErr := reader.ReadString('\n')
						if readErr != nil {
							err = readErr
							break
						}
						if dataLine == ".\r\n" {
							break
						}
						message.WriteString(dataLine)
					}
				}
				if err == nil {
					received <- message.String()
					err = write("250 queued\r\n")
				}
			case command == "QUIT":
				if err = write("221 bye\r\n"); err == nil {
					errors <- nil
				}
				return
			default:
				err = write("500 unsupported\r\n")
			}
			if err != nil {
				errors <- err
				return
			}
		}
	}()
	return listener.Addr().String(), received, errors
}

func messageHeaderValue(message, name string) string {
	prefix := name + ": "
	for _, line := range strings.Split(message, "\r\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}
