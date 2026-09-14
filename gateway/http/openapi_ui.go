package http

import (
	"bytes"
	"html/template"
	stdhttp "net/http"
	"strconv"
)

// Adapted from original Datly gateway/route_doc.go (Apache-2.0). The optional
// page uses original Swagger UI 5 CDN assets; offline asset hosting is separate.
const documentUI = `<!DOCTYPE html>
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
        url: {{.}},
        dom_id: "#swagger-ui",
        deepLinking: true,
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
        layout: "StandaloneLayout"
    });
};
</script>
</body>
</html>`

func (d *documentRoutes) uiDocument() ([]byte, error) {
	page, err := template.New("openapi").Parse(documentUI)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	err = page.Execute(&buffer, d.prefix)
	return buffer.Bytes(), err
}

func (d *documentRoutes) serveUI(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
	if request.Method != "GET" && request.Method != "HEAD" {
		writer.Header().Set("Allow", "GET, HEAD")
		writer.WriteHeader(405)
		return
	}
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.Header().Set("Content-Length", strconv.Itoa(len(d.ui)))
	writer.WriteHeader(200)
	if request.Method == "GET" {
		_, _ = writer.Write(d.ui)
	}
}
