package gateway

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository/contract"
	"net/http"
)

// swaggerUITemplate renders a minimal Swagger UI page loaded from a public CDN.
// %s is replaced with the URL of the aggregate OpenAPI spec (JSON).
const swaggerUITemplate = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Datly API</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
    <style>body { margin: 0; background: #fafafa; }</style>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js" crossorigin></script>
<script>
window.onload = function () {
    window.ui = SwaggerUIBundle({
        url: %q,
        dom_id: "#swagger-ui",
        deepLinking: true,
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
        layout: "StandaloneLayout"
    });
};
</script>
</body>
</html>`

// NewOpenAPIDocRoute serves an interactive Swagger UI page that renders the
// aggregate OpenAPI spec located at specURL.
func (r *Router) NewOpenAPIDocRoute(URL string, specURL string) *Route {
	page := []byte(fmt.Sprintf(swaggerUITemplate, specURL))
	return &Route{
		Path: contract.NewPath(http.MethodGet, URL),
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			setContentType(response, http.StatusOK, "text/html")
			write(response, http.StatusOK, page)
		},
		Kind:    RouteOpenAPIKind,
		Config:  r.config.Logging,
		Version: r.config.Version,
	}
}
