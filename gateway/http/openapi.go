package http

import (
	"fmt"
	stdhttp "net/http"
	"strings"

	"github.com/viant/datly/gateway/openapi"
)

type documentRoute struct {
	snapshot *openapi.Snapshot
	handler  *openapi.Handler
	access   DocumentAccess
}

// documentRoutes holds only frozen representations, not another component
// registry. Source-path matching continues through the serving runtime.
type documentRoutes struct {
	prefix, apiPrefix, uiPath string
	aggregate                 *documentRoute
	paths                     map[string]*documentRoute
	cors                      *corsPolicy
	ui                        []byte
}

func (h *Handler) ExportOpenAPI(request openapi.ExportRequest) ([]byte, error) {
	if h == nil || h.documents == nil {
		return nil, fmt.Errorf("OpenAPI publication is not configured")
	}
	selected := h.documents.aggregate
	if request.Path != "" {
		selected = h.documents.paths[request.Path]
		if request.Format == "" {
			request.Format = "yaml"
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("public OpenAPI path group not found: %s", request.Path)
	}
	return selected.snapshot.Export(request.Format)
}

func (h *Handler) serveDocuments(writer stdhttp.ResponseWriter, req *stdhttp.Request) bool {
	d := h.documents
	path := req.URL.EscapedPath()
	isUI := d.uiPath != "" && path == d.uiPath
	if !isUI && path != d.prefix && !strings.HasPrefix(path, d.prefix+"/") {
		return false
	}
	selected := d.aggregate
	if !isUI && path != d.prefix {
		source := d.apiPrefix + strings.TrimPrefix(path, d.prefix)
		selected = nil
		for _, method := range h.runtime.AllowedMethodsForPath(source) {
			endpoint, ok := h.runtime.RouteByMethodPath(method, source)
			if !ok {
				continue
			}
			candidate := d.paths[endpoint.Path]
			if candidate != nil && selected != nil && candidate != selected {
				writer.WriteHeader(404)
				return true
			}
			if candidate != nil {
				selected = candidate
			}
		}
		if selected == nil {
			writer.WriteHeader(404)
			return true
		}
	}
	preflight := req.Method == "OPTIONS" && req.Header.Get("Origin") != "" && req.Header.Get("Access-Control-Request-Method") != ""
	if preflight {
		method := req.Header.Get("Access-Control-Request-Method")
		if (method == "GET" || method == "HEAD") && d.cors.apply(writer, req, method, true) {
			writer.WriteHeader(204)
		} else {
			writer.WriteHeader(403)
		}
		return true
	}
	writer = (&corsResponseWriter{ResponseWriter: writer, policy: d.cors, request: req, method: req.Method}).wrap()
	if selected.access.APIKeyHeader != "" && selected.access.Authorize(req) != nil {
		writer.WriteHeader(403)
		return true
	}
	if isUI {
		d.serveUI(writer, req)
	} else {
		selected.handler.ServeHTTP(writer, req)
	}
	return true
}
