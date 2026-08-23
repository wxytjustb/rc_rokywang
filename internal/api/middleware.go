package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"notification-delivery/internal/authn"
)

// bearerAuth checks whether the caller supplied one of the configured tokens.
// Tokens authorize access to the API and do not encode a source_system.
func bearerAuth(verifier *authn.Verifier) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			writeError(c, http.StatusUnauthorized, errUnauthenticated)
			return
		}
		token := strings.TrimPrefix(header, prefix)
		if err := verifier.Verify(token); err != nil {
			writeError(c, http.StatusUnauthorized, errUnauthenticated)
			return
		}
		c.Next()
	}
}
