package api

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	_ "notification-delivery/docs"
	"notification-delivery/internal/application/notification"
	"notification-delivery/internal/authn"
)

type Deps struct {
	Service        *notification.Service
	AuthVerifier   *authn.Verifier
	Logger         *slog.Logger
	AuthTokens     []string
	MaxBodyBytes   int64
	SwaggerEnabled bool
	// Ready is polled by GET /readyz; it should check dependencies the
	// process needs to serve traffic (e.g. a DB ping) so a load balancer
	// can stop routing to an instance that lost its database connection
	// without killing the process.
	Ready func() error
}

func maxBodyBytes(limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if limit > 0 {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
		}
		c.Next()
	}
}

// healthz reports whether the HTTP process is alive.
//
// @Summary Liveness probe
// @Description Returns HTTP 200 when the API process is running.
// @Tags system
// @Produce json
// @Param Accept-Language header string false "Error message language: zh-CN or en-US"
// @Success 200 {object} emptyAPIResponse
// @Router /healthz [get]
func healthz(c *gin.Context) {
	writeSuccess(c, http.StatusOK, emptyData{})
}

// readyz reports whether the process dependencies are ready to serve traffic.
//
// @Summary Readiness probe
// @Description Checks the dependencies required to serve API traffic.
// @Tags system
// @Produce json
// @Param Accept-Language header string false "Error message language: zh-CN or en-US"
// @Success 200 {object} readinessAPIResponse
// @Failure 503 {object} errorResponse
// @Router /readyz [get]
func readyz(ready func() error) gin.HandlerFunc {
	return func(c *gin.Context) {
		if ready != nil {
			if err := ready(); err != nil {
				writeError(c, http.StatusServiceUnavailable, errDependencyUnavailable)
				return
			}
		}
		writeSuccess(c, http.StatusOK, readinessData{Ready: true})
	}
}

// NewRouter builds the gin engine for the notification-delivery API
// (DESIGN.md §4.1, §4.2). In memory mode its Publisher points at the
// server-local worker channel; database atomic claims still make duplicate
// wakes safe when multiple server instances run behind a load balancer.
func NewRouter(d Deps) *gin.Engine {
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.RedirectTrailingSlash = false
	r.Use(accessLogger(d.Logger))
	r.Use(gin.CustomRecovery(func(c *gin.Context, _ any) {
		writeError(c, http.StatusInternalServerError, errInternal)
	}))
	r.Use(maxBodyBytes(d.MaxBodyBytes))
	r.NoRoute(func(c *gin.Context) {
		writeError(c, http.StatusNotFound, errRouteNotFound)
	})
	r.NoMethod(func(c *gin.Context) {
		writeError(c, http.StatusMethodNotAllowed, errMethodNotAllowed)
	})

	r.GET("/healthz", healthz)
	r.GET("/readyz", readyz(d.Ready))
	if d.SwaggerEnabled {
		defaultBearerToken := ""
		if len(d.AuthTokens) > 0 {
			defaultBearerToken = d.AuthTokens[0]
		}
		r.GET("/docs", func(c *gin.Context) {
			c.Redirect(http.StatusTemporaryRedirect, "/docs/index.html")
		})
		r.GET("/docs/*any", swaggerHandler(defaultBearerToken))
	}

	h := &Handlers{service: d.Service}

	v1 := r.Group("/v1", bearerAuth(d.AuthVerifier))
	v1.GET("/providers", h.listProviders)
	v1.POST("/messages", h.createMessage)
	v1.GET("/messages/:source_request_id", h.getMessage)

	return r
}
