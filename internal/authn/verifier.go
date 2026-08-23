// Package authn provides protocol-neutral authentication for the HTTP, gRPC,
// and MCP server adapters.
package authn

import "errors"

// ErrInvalidToken is returned when a bearer token is missing or unknown.
var ErrInvalidToken = errors.New("invalid bearer token")

// Verifier authenticates the static bearer tokens configured for the server.
// Tokens authorize service access only; they do not identify a source system.
type Verifier struct {
	accepted map[string]struct{}
}

// NewVerifier builds an immutable verifier from the effective token list.
func NewVerifier(tokens []string) *Verifier {
	accepted := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		if token != "" {
			accepted[token] = struct{}{}
		}
	}
	return &Verifier{accepted: accepted}
}

// Verify returns nil only for a configured token.
func (v *Verifier) Verify(token string) error {
	if v == nil {
		return ErrInvalidToken
	}
	if _, ok := v.accepted[token]; !ok {
		return ErrInvalidToken
	}
	return nil
}
