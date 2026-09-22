package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/url"
	"strings"
	"time"

	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
)

const DefaultCacheInvalidateURI = "/v1/api/cache/invalidate"

// CacheInvalidationConfig enables cache administration independently of warmup.
// Authorize must grant administrator access to this exact server-selected target.
type CacheInvalidationConfig struct {
	Timeout   time.Duration
	Authorize func(context.Context, *stdhttp.Request, dexec.ComponentTarget) error
}
type cacheRoute struct {
	target dexec.ComponentTarget
	route  *spec.Route
	cors   *corsPolicy
}
type cacheRoutes struct {
	prefix, apiPrefix string
	routes            map[string]*cacheRoute
	policy            CacheInvalidationConfig
}

func newCacheRoutes(rt *druntime.Runtime, c Config) (*cacheRoutes, error) {
	if c.CacheInvalidation == nil || strings.TrimSpace(c.Meta.CacheInvalidateURI) == "" {
		return nil, nil
	}
	if c.CacheInvalidation.Authorize == nil || c.CacheInvalidation.Timeout <= 0 {
		return nil, fmt.Errorf("cache invalidation requires administrator authorization and a positive timeout")
	}
	prefix := strings.TrimSuffix(c.Meta.CacheInvalidateURI, "/")
	apiPrefix := strings.TrimSuffix(c.APIPrefix, "/")
	for _, value := range []string{prefix, apiPrefix} {
		if value == "" {
			continue
		}
		parsed, err := url.Parse(value)
		if err != nil || !strings.HasPrefix(value, "/") || parsed.RawQuery != "" || parsed.Fragment != "" || strings.ContainsAny(value, "{}%*\r\n ") {
			return nil, fmt.Errorf("invalid cache administration prefix %q", value)
		}
	}
	for _, reserved := range []string{c.Meta.CacheWarmURI, c.Meta.OpenApiURI, c.Meta.DocURI} {
		reserved = strings.TrimSuffix(strings.TrimSpace(reserved), "/")
		if reserved != "" && (prefix == reserved || strings.HasPrefix(prefix, reserved+"/") || strings.HasPrefix(reserved, prefix+"/")) {
			return nil, fmt.Errorf("cache invalidation namespace overlaps metadata route %s", reserved)
		}
	}
	if prefix == "" || prefix == apiPrefix {
		return nil, fmt.Errorf("cache invalidation prefix must be distinct")
	}
	result := &cacheRoutes{prefix: prefix, apiPrefix: apiPrefix, routes: map[string]*cacheRoute{}, policy: *c.CacheInvalidation}
	for _, endpoint := range rt.CacheRoutes() {
		if apiPrefix != "" && endpoint.Path != apiPrefix && !strings.HasPrefix(endpoint.Path, apiPrefix+"/") {
			continue
		}
		target, ok := rt.CacheTarget(endpoint.Path)
		if !ok {
			continue
		}
		policy, err := newCORSPolicy(c.routeCORS(endpoint))
		if err != nil {
			return nil, err
		}
		result.routes[endpoint.Path] = &cacheRoute{target: target, route: endpoint, cors: policy}
	}
	for _, endpoint := range rt.Routes() {
		if endpoint.Path == prefix || strings.HasPrefix(endpoint.Path, prefix+"/") {
			return nil, fmt.Errorf("cache invalidation namespace collides with component route %s", endpoint.Path)
		}
	}
	return result, nil
}
func (h *Handler) serveCacheInvalidation(writer stdhttp.ResponseWriter, req *stdhttp.Request) bool {
	routes := h.cacheInvalidation
	path := req.URL.EscapedPath()
	if path != routes.prefix && !strings.HasPrefix(path, routes.prefix+"/") {
		return false
	}
	targetPath := routes.apiPrefix + strings.TrimPrefix(path, routes.prefix)
	target, ok := h.runtime.CacheTarget(targetPath)
	requestRoute, routeOK := h.runtime.CacheRoute(targetPath)
	endpoint := routes.routes[requestRoute.Path]
	if !ok || !routeOK || endpoint == nil || endpoint.target != target {
		writeJSON(writer, 404, map[string]string{"status": "error", "message": "cache target not found"})
		return true
	}
	if req.Method == "OPTIONS" && req.Header.Get("Origin") != "" && req.Header.Get("Access-Control-Request-Method") != "" {
		if req.Header.Get("Access-Control-Request-Method") == "POST" && endpoint.cors.apply(writer, req, "POST", true) {
			writer.WriteHeader(204)
		} else {
			writer.WriteHeader(403)
		}
		return true
	}
	endpoint.cors.apply(writer, req, "POST", false)
	if req.Method != "POST" {
		writer.Header().Set("Allow", "POST")
		writer.WriteHeader(405)
		return true
	}
	if endpoint.route.APIKeyHeader != "" && req.Header.Get(endpoint.route.APIKeyHeader) != endpoint.route.APIKeyValue {
		writer.WriteHeader(403)
		return true
	}
	ctx, cancel := context.WithTimeout(req.Context(), routes.policy.Timeout)
	defer cancel()
	req = req.WithContext(ctx)
	if ctx.Err() != nil {
		writer.WriteHeader(408)
		return true
	}
	if h.authorize != nil {
		if err := h.authorize(ctx, req, target); err != nil {
			writer.WriteHeader(403)
			return true
		}
	}
	if err := routes.policy.Authorize(ctx, req, target); err != nil {
		writer.WriteHeader(403)
		return true
	}
	input := &struct {
		View  string `json:"view"`
		Scope string `json:"scope"`
	}{Scope: "all"}
	body := req.Body
	if body == nil {
		body = stdhttp.NoBody
	}
	decoder := json.NewDecoder(stdhttp.MaxBytesReader(writer, body, 8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil && err != io.EOF {
		writeJSON(writer, 400, map[string]string{"status": "error", "message": err.Error()})
		return true
	}
	if input == nil {
		writer.WriteHeader(400)
		return true
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		writer.WriteHeader(400)
		return true
	}
	if err := dexec.ValidateCacheScope(input.Scope); err != nil {
		writeJSON(writer, 400, map[string]string{"status": "error", "message": err.Error()})
		return true
	}
	entries, err := h.runtime.InvalidateCache(ctx, target, input.View, input.Scope)
	status := 200
	result := map[string]interface{}{"status": "ok", "invalidated": entries}
	if err != nil {
		status = 500
		if errors.Is(err, dexec.ErrCacheViewNotFound) {
			status = 404
		}
		result["status"] = "error"
		result["message"] = err.Error()
	}
	writeJSON(writer, status, result)
	return true
}
