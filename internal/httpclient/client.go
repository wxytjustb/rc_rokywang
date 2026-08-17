// Package httpclient is the single place outbound vendor HTTP calls go
// through, so the size limits, TLS verification and timeout behavior
// required by DESIGN.md §11.1 are enforced once instead of per adapter.
package httpclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

type Client struct {
	inner            *http.Client
	maxResponseBytes int64
}

// New builds a client with certificate verification on (Go's default —
// InsecureSkipVerify is never set) and a hard cap on how much response body
// is ever read into memory.
func New(maxResponseBytes int64) *Client {
	if maxResponseBytes <= 0 {
		maxResponseBytes = 1 << 20 // 1 MiB default
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		Proxy:           http.ProxyFromEnvironment,
	}
	return &Client{
		inner:            &http.Client{Transport: transport},
		maxResponseBytes: maxResponseBytes,
	}
}

type Request struct {
	Method  string
	URL     string
	Headers map[string]string
	Body    []byte
	Timeout time.Duration
}

type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
	Truncated  bool
}

// Do issues the request under a hard per-call timeout. The returned error,
// when non-nil, can be passed to PreSendFailure for diagnostic logging about
// whether the vendor could possibly have observed the request.
func (c *Client) Do(ctx context.Context, req Request) (*Response, error) {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return nil, err
	}
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := c.inner.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	limited := io.LimitReader(resp.Body, c.maxResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	truncated := false
	if int64(len(data)) > c.maxResponseBytes {
		data = data[:c.maxResponseBytes]
		truncated = true
	}
	return &Response{
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
		Body:       data,
		Truncated:  truncated,
	}, nil
}

// PreSendFailure reports whether err happened while still establishing the
// TCP/TLS connection — i.e. before any request bytes could have reached the
// vendor. Errors past that point (write failed mid-request, response
// timed out, connection reset while waiting) cannot rule out the vendor
// having received and even processed the request, so callers must treat
// those as possibly observed by the vendor. Both cases are retried by the
// worker; the distinction is retained only for diagnostics.
func PreSendFailure(err error) bool {
	if err == nil {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial"
	}
	return false
}
