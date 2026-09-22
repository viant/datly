package http

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/packageasset"
	"github.com/viant/datly/spec"
	"io/fs"
	stdhttp "net/http"
	"path"
	"strings"
)

type staticRoute struct {
	prefix  string
	content *spec.StaticContent
	handler stdhttp.Handler
	cors    *corsPolicy
	keys    APIKeys
}

func (c Config) staticRoutes(ctx context.Context, input HandlerInput, keys APIKeys) ([]*staticRoute, error) {
	var result []*staticRoute
	for _, configured := range c.StaticContent {
		content := configured.Clone()
		if err := content.Validate(); err != nil {
			return nil, err
		}
		prefix := strings.TrimSuffix(content.Path, "/")
		for _, previous := range result {
			if prefix == previous.prefix {
				return nil, fmt.Errorf("duplicate static prefix %q", content.Path)
			}
			if strings.HasPrefix(prefix, previous.prefix+"/") || strings.HasPrefix(previous.prefix, prefix+"/") {
				return nil, fmt.Errorf("overlapping static prefixes")
			}
		}
		if len(input.Runtime.AllowedMethodsForPath(content.Path)) > 0 {
			return nil, fmt.Errorf("static prefix conflicts with component path %s", content.Path)
		}
		for _, endpoint := range input.Runtime.Routes() {
			if strings.TrimSuffix(endpoint.Path, "/") == prefix {
				return nil, fmt.Errorf("static prefix conflicts with component route %s", endpoint.Path)
			}
		}
		for _, reserved := range []string{c.Meta.OpenApiURI, c.Meta.DocURI, c.Meta.CacheWarmURI, c.Meta.CacheInvalidateURI} {
			reserved = strings.TrimSuffix(reserved, "/")
			if reserved != "" && (prefix == reserved || strings.HasPrefix(prefix, reserved+"/")) {
				return nil, fmt.Errorf("static prefix conflicts with metadata/admin route %s", reserved)
			}
		}
		if err := (&DocumentAccess{APIKeyHeader: content.APIKeyHeader, APIKeyValue: content.APIKeyValue}).Validate(); err != nil && (content.APIKeyHeader != "" || content.APIKeyValue != "") {
			return nil, err
		}
		snapshot, err := (packageasset.StaticSource{LocalRoot: c.StaticLocalRoot, Resources: input.Runtime.Resources(), ContentURL: c.ContentURL}).Snapshot(ctx, content)
		if err != nil {
			return nil, fmt.Errorf("static %s: %w", content.Path, err)
		}
		if _, err = fs.Stat(snapshot, "."); err != nil {
			return nil, err
		}
		policy, err := newCORSPolicy(c.routeCORS(&spec.Route{CORS: content.CORS}))
		if err != nil {
			return nil, err
		}
		result = append(result, &staticRoute{prefix: prefix, content: content, handler: stdhttp.FileServerFS(snapshot), cors: policy, keys: keys})
	}
	return result, nil
}

func (h *Handler) serveStatic(writer stdhttp.ResponseWriter, req *stdhttp.Request) bool {
	if len(h.static) == 0 {
		return false
	}
	// Any component path owns all methods, including its 405 and CORS behavior.
	if len(h.runtime.AllowedMethodsForPath(req.URL.EscapedPath())) > 0 || len(h.runtime.AllowedMethodsForPath(req.URL.Path)) > 0 {
		return false
	}
	for _, route := range h.static {
		name := req.URL.Path
		if name != route.prefix && !strings.HasPrefix(name, route.prefix+"/") {
			continue
		}
		relative := strings.TrimPrefix(name, route.prefix)
		// Reject dot segments before FileServer can clean them. Decode only once:
		// URL.Path is net/http's decoded path; RawPath is never used as a file name.
		check := strings.TrimSuffix(strings.TrimPrefix(relative, "/"), "/")
		if check == "." || strings.Contains(relative, "//") || check != "" && (!fs.ValidPath(check) || path.Clean(check) != check) || strings.ContainsAny(relative, "\\\x00") {
			stdhttp.NotFound(writer, req)
			return true
		}
		if h.warmup != nil && (req.URL.Path == h.warmup.prefix || strings.HasPrefix(req.URL.Path, h.warmup.prefix+"/")) {
			stdhttp.NotFound(writer, req)
			return true
		}
		if h.documents != nil {
			d := h.documents
			if req.URL.Path == d.uiPath || req.URL.Path == d.prefix || strings.HasPrefix(req.URL.Path, d.prefix+"/") {
				stdhttp.NotFound(writer, req)
				return true
			}
		}
		escaped := strings.ToLower(req.URL.EscapedPath())
		if strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") {
			stdhttp.NotFound(writer, req)
			return true
		}
		method := req.Method
		preflight := method == stdhttp.MethodOptions && req.Header.Get("Origin") != "" && req.Header.Get("Access-Control-Request-Method") != ""
		if method == stdhttp.MethodOptions && !preflight && route.cors != nil {
			var allowed []string
			for _, candidate := range []string{stdhttp.MethodGet, stdhttp.MethodHead} {
				if route.cors.config.AllowMethods == nil || route.cors.allows(route.cors.config.AllowMethods, candidate, false) {
					allowed = append(allowed, candidate)
				}
			}
			if len(allowed) > 0 {
				writer.Header().Set("Allow", strings.Join(append(allowed, "OPTIONS"), ", "))
				writer.WriteHeader(stdhttp.StatusNoContent)
				return true
			}
		}
		if preflight {
			method = req.Header.Get("Access-Control-Request-Method")
		}
		if method != stdhttp.MethodGet && method != stdhttp.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD, OPTIONS")
			writer.WriteHeader(stdhttp.StatusMethodNotAllowed)
			return true
		}
		if preflight {
			if route.cors.apply(writer, req, method, true) {
				writer.WriteHeader(stdhttp.StatusNoContent)
			} else {
				writer.WriteHeader(stdhttp.StatusForbidden)
			}
			return true
		}
		route.cors.apply(writer, req, method, false)
		header, value := route.content.APIKeyHeader, route.content.APIKeyValue
		// Longest configured key prefix also protects subpaths inside a static tree.
		if key := route.keys.match(req.URL.Path); key != nil {
			header, value = key.Header, key.Value
		}
		if header != "" && (&DocumentAccess{APIKeyHeader: header, APIKeyValue: value}).Authorize(req) != nil {
			writer.WriteHeader(stdhttp.StatusForbidden)
			return true
		}
		if relative == "" {
			target := req.URL.EscapedPath() + "/"
			if req.URL.RawQuery != "" {
				target += "?" + req.URL.RawQuery
			}
			stdhttp.Redirect(writer, req, target, stdhttp.StatusMovedPermanently)
			return true
		}
		cloned := req.Clone(req.Context())
		cloned.URL.Path = relative
		cloned.URL.RawPath = ""
		cloned.RequestURI = cloned.URL.RequestURI()
		route.handler.ServeHTTP(writer, cloned)
		return true
	}
	return false
}
