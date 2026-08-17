package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"notification-delivery/internal/domain"
	"notification-delivery/internal/httpclient"
)

// buildProviderResponse implements the sanitization rules in DESIGN.md
// §3.1: only allow-listed headers survive, JSON bodies are kept as JSON,
// text bodies as strings, binary bodies are represented by length+digest
// only, and truncation is flagged rather than silently hidden.
func buildProviderResponse(resp *httpclient.Response, allowedHeaders []string) (json.RawMessage, error) {
	if resp == nil {
		return nil, nil
	}
	contentType := resp.Header.Get("Content-Type")

	pr := domain.ProviderResponse{
		HTTPStatus:  resp.StatusCode,
		ContentType: contentType,
		Headers:     filterHeaders(resp.Header, allowedHeaders),
		ReceivedAt:  time.Now().UTC(),
		Truncated:   resp.Truncated,
	}

	switch {
	case isJSONContentType(contentType) && json.Valid(resp.Body):
		pr.Body = json.RawMessage(resp.Body)
	case isTextualContentType(contentType):
		asString, err := json.Marshal(string(resp.Body))
		if err != nil {
			return nil, err
		}
		pr.Body = asString
	default:
		pr.ContentLength = int64(len(resp.Body))
		sum := sha256.Sum256(resp.Body)
		pr.BodyDigest = "sha256:" + hex.EncodeToString(sum[:])
	}

	return json.Marshal(pr)
}

func isJSONContentType(ct string) bool {
	ct = strings.ToLower(ct)
	return strings.Contains(ct, "json")
}

func isTextualContentType(ct string) bool {
	if ct == "" {
		return true // unspecified — most vendor mock APIs omit it; default to text-safe handling
	}
	ct = strings.ToLower(ct)
	return strings.HasPrefix(ct, "text/") || strings.Contains(ct, "json") || strings.Contains(ct, "xml") || strings.Contains(ct, "urlencoded")
}

func filterHeaders(h http.Header, allowed []string) map[string]string {
	if len(allowed) == 0 {
		return nil
	}
	out := make(map[string]string)
	for _, name := range allowed {
		if v := h.Get(name); v != "" {
			out[name] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
