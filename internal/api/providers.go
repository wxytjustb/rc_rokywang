package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// listProviders exposes the runtime-enabled provider/action capabilities.
//
// @Summary List supported provider capabilities
// @Description Returns every runtime-enabled provider_code and provider_action with a Chinese functional description, suitable for agent or MCP capability discovery.
// @Tags providers
// @Produce json
// @Security BearerAuth
// @Param Accept-Language header string false "Error message language: zh-CN or en-US"
// @Success 200 {object} providerCapabilitiesAPIResponse
// @Failure 401 {object} errorResponse
// @Router /v1/providers [get]
func (h *Handlers) listProviders(c *gin.Context) {
	capabilities := h.service.ListCapabilities(c.Request.Context())
	providers := make([]providerCapability, 0, len(capabilities))
	for _, capability := range capabilities {
		actions := make([]providerActionCapability, 0, len(capability.Actions))
		for _, action := range capability.Actions {
			actions = append(actions, providerActionCapability{
				ProviderAction: action.ProviderAction,
				Description:    action.Description,
			})
		}
		providers = append(providers, providerCapability{
			ProviderCode: capability.ProviderCode,
			Actions:      actions,
		})
	}
	writeSuccess(c, http.StatusOK, providerCapabilitiesData{Providers: providers})
}
