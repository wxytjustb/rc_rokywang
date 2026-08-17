package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// bearerAuth checks whether the caller supplied one of the configured tokens.
// Tokens authorize access to the API and do not encode a source_system.
func bearerAuth(tokens []string) gin.HandlerFunc {
	accepted := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		accepted[token] = struct{}{}
	}

	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			writeError(c, http.StatusUnauthorized, errUnauthenticated)
			return
		}
		token := strings.TrimPrefix(header, prefix)
		_, ok := accepted[token]
		if !ok {
			writeError(c, http.StatusUnauthorized, errUnauthenticated)
			return
		}
		c.Next()
	}
}
