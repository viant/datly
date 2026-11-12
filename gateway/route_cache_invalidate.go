package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/viant/datly/internal/cache/managed"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/view"
	"github.com/viant/xdatly/handler/exec"
)

type cacheInvalidationRequest struct {
	View  string        `json:"view,omitempty"`
	Scope managed.Scope `json:"scope,omitempty"`
}
type cacheInvalidationEntry struct {
	URI        string        `json:"uri"`
	View       string        `json:"view"`
	Scope      managed.Scope `json:"scope"`
	Generation string        `json:"generation,omitempty"`
	Error      string        `json:"error,omitempty"`
}
type cacheInvalidationResponse struct {
	Status      string                   `json:"status"`
	Error       string                   `json:"error,omitempty"`
	Invalidated []cacheInvalidationEntry `json:"invalidated,omitempty"`
}

func (r *Router) appendCacheInvalidationRoute(routes []*Route, aPath *path.Path, provider *repository.Provider) []*Route {
	if aPath.Method != http.MethodGet || strings.TrimSpace(r.config.Meta.CacheInvalidateURI) == "" {
		return routes
	}
	return append(routes, r.NewCacheInvalidationRoute(r.routeURL(r.config.Meta.CacheInvalidateURI, aPath.URI), provider))
}
func (r *Router) NewCacheInvalidationRoute(uri string, providers ...*repository.Provider) *Route {
	return &Route{
		Path: contract.NewPath(http.MethodPost, uri), Providers: providers,
		Kind: RouteCacheInvalidationKind, Config: r.config.Logging, Version: r.config.Version,
		Handler: func(ctx context.Context, writer http.ResponseWriter, req *http.Request) {
			r.handleCacheInvalidation(ctx, writer, req, providers)
		},
	}
}
func writeCacheInvalidation(ctx context.Context, writer http.ResponseWriter, status int, result cacheInvalidationResponse) {
	if execution, ok := ctx.Value(exec.ContextKey).(*exec.Context); ok {
		execution.StatusCode = status
	}
	setContentType(writer, status, "application/json")
	data, _ := json.Marshal(result)
	write(writer, status, data)
}
func (r *Router) handleCacheInvalidation(ctx context.Context, writer http.ResponseWriter, req *http.Request, providers []*repository.Provider) {
	input := &cacheInvalidationRequest{Scope: managed.All}
	if req.Body != nil {
		decoder := json.NewDecoder(http.MaxBytesReader(writer, req.Body, 8192))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil && err != io.EOF {
			writeCacheInvalidation(ctx, writer, http.StatusBadRequest, cacheInvalidationResponse{Status: "error", Error: err.Error()})
			return
		}
		if input == nil {
			writeCacheInvalidation(ctx, writer, http.StatusBadRequest, cacheInvalidationResponse{Status: "error", Error: "expected a JSON object"})
			return
		}
		var extra interface{}
		if err := decoder.Decode(&extra); err != io.EOF {
			writeCacheInvalidation(ctx, writer, http.StatusBadRequest, cacheInvalidationResponse{Status: "error", Error: "expected one JSON object"})
			return
		}
	}
	if err := input.Scope.Validate(); err != nil {
		writeCacheInvalidation(ctx, writer, http.StatusBadRequest, cacheInvalidationResponse{Status: "error", Error: err.Error()})
		return
	}
	type target struct {
		uri  string
		view *view.View
	}
	var targets []target
	seen := map[*view.Cache]bool{}
	visited := map[*view.View]bool{}
	// Resolve every target before performing any mutations.
	for _, provider := range providers {
		component, err := provider.Component(ctx)
		if err != nil {
			writeCacheInvalidation(ctx, writer, http.StatusInternalServerError, cacheInvalidationResponse{Status: "error", Error: err.Error()})
			return
		}
		if component == nil {
			writeCacheInvalidation(ctx, writer, http.StatusNotFound, cacheInvalidationResponse{Status: "error", Error: "component not found"})
			return
		}
		var visit func(*view.View)
		visit = func(v *view.View) {
			if v == nil || visited[v] {
				return
			}
			visited[v] = true
			if v.Cache != nil && !seen[v.Cache] && (input.View == "" || input.View == v.Name) {
				seen[v.Cache] = true
				targets = append(targets, target{component.URI, v})
			}
			for _, relation := range v.With {
				if relation != nil && relation.Of != nil {
					visit(&relation.Of.View)
				}
			}
		}
		visit(component.View)
	}
	if len(targets) == 0 {
		writeCacheInvalidation(ctx, writer, http.StatusNotFound, cacheInvalidationResponse{Status: "error", Error: "no matching cached views"})
		return
	}
	result := cacheInvalidationResponse{Status: "ok"}
	status := http.StatusOK
	for _, target := range targets {
		entry := cacheInvalidationEntry{URI: target.uri, View: target.view.Name, Scope: input.Scope}
		token, err := target.view.Cache.InvalidateCache(ctx, string(input.Scope))
		if err != nil {
			entry.Error = err.Error()
			result.Status = "error"
			status = http.StatusInternalServerError
		} else {
			entry.Generation = token
		}
		result.Invalidated = append(result.Invalidated, entry)
	}
	writeCacheInvalidation(ctx, writer, status, result)
}
