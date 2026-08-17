package api

import (
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"

	"notification-delivery/internal/provider"
)

// listProviders exposes the runtime-enabled provider/action capabilities.
// Descriptions come from the owning adapter's normalized Config, so this
// endpoint cannot drift from the actions the server actually accepts.
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
	writeSuccess(c, http.StatusOK, buildProviderCapabilities(h.registry.All()))
}

func buildProviderCapabilities(resolved map[string]provider.ResolvedAction) providerCapabilitiesData {
	grouped := make(map[string][]providerActionCapability)
	for _, action := range resolved {
		providerCode := action.Context.ProviderCode
		grouped[providerCode] = append(grouped[providerCode], providerActionCapability{
			ProviderAction: action.Context.ProviderAction,
			Description:    action.Description,
		})
	}

	providerCodes := make([]string, 0, len(grouped))
	for providerCode := range grouped {
		providerCodes = append(providerCodes, providerCode)
	}
	sort.Strings(providerCodes)

	providers := make([]providerCapability, 0, len(providerCodes))
	for _, providerCode := range providerCodes {
		actions := grouped[providerCode]
		sort.Slice(actions, func(i, j int) bool {
			return actions[i].ProviderAction < actions[j].ProviderAction
		})
		providers = append(providers, providerCapability{
			ProviderCode: providerCode,
			Actions:      actions,
		})
	}
	return providerCapabilitiesData{Providers: providers}
}
