package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func swaggerHandler(defaultBearerToken string) gin.HandlerFunc {
	assetHandler := ginSwagger.WrapHandler(
		swaggerFiles.Handler,
		ginSwagger.PersistAuthorization(true),
	)
	tokenJSON, _ := json.Marshal(defaultBearerToken)
	indexHTML := fmt.Sprintf(swaggerIndexHTML, tokenJSON)

	return func(c *gin.Context) {
		if c.Param("any") == "/index.html" {
			c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(indexHTML))
			return
		}
		assetHandler(c)
	}
}

const swaggerIndexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Notification Delivery API</title>
  <link rel="stylesheet" href="./swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="./swagger-ui-bundle.js"></script>
<script src="./swagger-ui-standalone-preset.js"></script>
<script>
window.onload = function () {
  const defaultBearerToken = %s;
  const ui = SwaggerUIBundle({
    url: "doc.json",
    dom_id: "#swagger-ui",
    validatorUrl: null,
    persistAuthorization: true,
    presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
    plugins: [SwaggerUIBundle.plugins.DownloadUrl],
    layout: "StandaloneLayout",
    requestInterceptor: function (request) {
      const authorization = request.headers && request.headers.Authorization;
      if (authorization && !authorization.startsWith("Bearer ")) {
        request.headers.Authorization = "Bearer " + authorization;
      }
      return request;
    }
  });
  if (defaultBearerToken) {
    ui.preauthorizeApiKey("BearerAuth", defaultBearerToken);
  }
  window.ui = ui;
};
</script>
</body>
</html>`
