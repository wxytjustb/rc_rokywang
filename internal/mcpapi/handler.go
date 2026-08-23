package mcpapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"notification-delivery/internal/application/notification"
	"notification-delivery/internal/authn"
)

type Options struct {
	MaxBodyBytes int64
}

// NewHandler returns an authenticated, origin-protected MCP Streamable HTTP
// endpoint. It intentionally uses stateless JSON responses because all
// notification tools are request/response operations.
func NewHandler(service *notification.Service, verifier *authn.Verifier, logger *slog.Logger, opts Options) http.Handler {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "notification-delivery",
		Version: "1.0.0",
	}, nil)
	registerTools(server, service)

	streamable := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
		Logger:       logger,
	})

	var handler http.Handler = streamable
	protection := http.NewCrossOriginProtection()
	handler = protection.Handler(handler)
	handler = auth.RequireBearerToken(func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		if err := verifier.Verify(token); err != nil {
			return nil, fmt.Errorf("%w: %v", auth.ErrInvalidToken, err)
		}
		// The SDK requires a future expiration even though these static tokens
		// are non-expiring and are revoked by removing them from configuration.
		// Recomputing a short horizon on every successful verification preserves
		// those service semantics without pretending the token has a fixed JWT
		// expiry.
		return &auth.TokenInfo{Expiration: time.Now().Add(24 * time.Hour)}, nil
	}, nil)(handler)
	handler = limitRequestBody(opts.MaxBodyBytes, handler)
	return accessLogger(logger, handler)
}

func limitRequestBody(limit int64, next http.Handler) http.Handler {
	if limit <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func accessLogger(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		wrapped := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(wrapped, r)
		status := wrapped.status
		if status == 0 {
			status = http.StatusOK
		}
		logger.InfoContext(r.Context(), "mcp request",
			"protocol", "mcp",
			"method", r.Method,
			"path", r.URL.Path,
			"status", status,
			"latency_ms", time.Since(startedAt).Milliseconds())
	})
}
