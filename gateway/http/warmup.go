package http

import (
	"fmt"
	stdhttp "net/http"
	"net/url"
	"strings"

	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
)

func writeWarmupNotFound(writer stdhttp.ResponseWriter) {
	writeJSON(writer, stdhttp.StatusNotFound, map[string]string{
		"status":  "error",
		"message": "No warmup target is configured for this route.",
	})
}

type warmupRoute struct {
	target                    dexec.ComponentTarget
	operation                 *druntime.Warmup
	credentials               []string
	cors                      *corsPolicy
	apiKeyHeader, apiKeyValue string
}

type warmupRoutes struct {
	prefix, apiPrefix string
	routes            map[string]*warmupRoute
	policy            WarmupConfig
	lifetime          *WarmupLifetime
}

type WarmupResult struct {
	Target string `json:"target"`
	Groups int    `json:"groups"`
	Status string `json:"status"`
}

func newWarmupRoutes(rt *druntime.Runtime, c Config) (*warmupRoutes, error) {
	prefix := strings.TrimSuffix(c.Meta.CacheWarmURI, "/")
	apiPrefix := strings.TrimSuffix(c.APIPrefix, "/")
	for _, path := range []string{prefix, apiPrefix} {
		if path == "" {
			continue
		}
		parsed, err := url.Parse(path)
		if err != nil || !strings.HasPrefix(path, "/") || parsed.RawQuery != "" || parsed.Fragment != "" || strings.ContainsAny(path, "{}%*") {
			return nil, fmt.Errorf("invalid HTTP prefix %q", path)
		}
	}
	if prefix == "" || prefix == apiPrefix {
		return nil, fmt.Errorf("warmup prefix must be distinct")
	}
	result := &warmupRoutes{prefix: prefix, apiPrefix: apiPrefix, routes: map[string]*warmupRoute{}}
	for _, endpoint := range rt.WarmupRoutes() {
		if !strings.EqualFold(endpoint.Method, "GET") {
			continue
		}
		if apiPrefix != "" && endpoint.Path != apiPrefix && !strings.HasPrefix(endpoint.Path, apiPrefix+"/") {
			continue
		}
		target, ok := rt.WarmupTarget(endpoint.Path)
		if !ok {
			continue
		}
		operation, err := rt.NewWarmup(target)
		if err != nil {
			return nil, err
		}
		credentials, err := operation.CredentialHeaders()
		if err != nil {
			return nil, err
		}
		policy, err := newCORSPolicy(c.routeCORS(endpoint))
		if err != nil {
			return nil, err
		}
		path := prefix + strings.TrimPrefix(endpoint.Path, apiPrefix)
		if len(rt.AllowedMethodsForPath(path)) > 0 {
			return nil, fmt.Errorf("warmup route collides with component route %s", path)
		}
		requestRoute := spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}
		result.routes[requestRoute.String()] = &warmupRoute{target: target, operation: operation, credentials: credentials, cors: policy, apiKeyHeader: endpoint.APIKeyHeader, apiKeyValue: endpoint.APIKeyValue}
	}
	if len(result.routes) == 0 {
		return nil, nil
	}
	for _, endpoint := range rt.Routes() {
		if endpoint.Path == prefix || strings.HasPrefix(endpoint.Path, prefix+"/") {
			return nil, fmt.Errorf("warmup namespace collides with component route %s", endpoint.Path)
		}
	}
	if c.Warmup == nil || !c.Warmup.Lifetime.available() || c.Warmup.Authorize == nil || c.Warmup.Timeout <= 0 {
		return nil, fmt.Errorf("HTTP warmup requires server lifetime, positive timeout and administrator authorization")
	}
	result.policy = *c.Warmup
	result.lifetime = c.Warmup.Lifetime
	return result, nil
}

func (h *Handler) serveWarmup(writer stdhttp.ResponseWriter, req *stdhttp.Request) bool {
	w := h.warmup
	path := req.URL.EscapedPath()
	if path != w.prefix && !strings.HasPrefix(path, w.prefix+"/") {
		return false
	}
	targetPath := w.apiPrefix + strings.TrimPrefix(path, w.prefix)
	target, ok := h.runtime.WarmupTarget(targetPath)
	requestRoute, routeOK := h.runtime.WarmupRoute(targetPath)
	endpoint := w.routes[requestRoute.String()]
	if !ok || !routeOK || endpoint == nil || endpoint.target != target {
		writeWarmupNotFound(writer)
		return true
	}
	preflight := req.Method == "OPTIONS" && req.Header.Get("Origin") != "" && req.Header.Get("Access-Control-Request-Method") != ""
	if preflight {
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
	if req.Context().Err() != nil {
		writer.WriteHeader(408)
		return true
	}
	ctx, finish, accepted := w.lifetime.begin(w.policy.Timeout)
	if !accepted {
		writer.WriteHeader(503)
		return true
	}
	defer finish()
	snapshot := w.snapshot(ctx, req, endpoint)
	result, status, err := endpoint.execute(ctx, w.policy, snapshot)
	if err != nil && h.logger != nil {
		h.logger.Error("HTTP cache warmup failed", err)
	}
	if w.policy.Completed != nil {
		w.policy.Completed(result, err)
	}
	writeJSON(writer, status, result)
	return true
}
