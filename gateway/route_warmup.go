package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"net/http"
)

func (r *Router) NewWarmupRoute(URL string, providers ...*repository.Provider) *Route {
	return &Route{
		Path:      contract.NewPath(http.MethodPost, URL),
		Providers: providers,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleCacheWarmup(ctx, response, providers)
		},
		Kind:    RouteWarmupKind,
		Config:  r.config.Logging,
		Version: r.config.Version,
		NewMultiRoute: func(routes []*contract.Path) *Route {
			return r.NewWarmupRoute("", providers...)
		},
	}
}

func (r *Router) handleCacheWarmup(ctx context.Context, writer http.ResponseWriter, provider []*repository.Provider) {
	statusCode, content := r.handleCacheWarmupWithErr(ctx, provider)
	setContentType(writer, statusCode, "application/json")
	write(writer, statusCode, content)
}

func (r *Router) handleCacheWarmupWithErr(ctx context.Context, providers []*repository.Provider) (int, []byte) {
	// HTTP-triggered warmup should survive client/LB timeout cancellation.
	warmupCtx := context.Background()
	viewsByURI := make(map[string][]*view.View, len(providers))
	URIs := make([]string, 0, len(providers))
	for _, provider := range providers {
		aComponent, err := provider.Component(warmupCtx)
		if err != nil {
			return http.StatusInternalServerError, []byte(err.Error())
		}
		if aComponent == nil {
			return http.StatusNotFound, []byte("component was not found")
		}
		views, err := router.ExtractCacheableViews(warmupCtx, aComponent)
		if err != nil {
			return http.StatusInternalServerError, []byte(err.Error())
		}
		if _, ok := viewsByURI[aComponent.URI]; !ok {
			URIs = append(URIs, aComponent.URI)
		}
		viewsByURI[aComponent.URI] = append(viewsByURI[aComponent.URI], views...)
	}

	lookup := func(_ context.Context, _, matchingURI string) ([]*view.View, error) {
		return viewsByURI[matchingURI], nil
	}
	response := warmup.PreCache(warmupCtx, lookup, URIs...)
	data, err := json.Marshal(response)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}
	return http.StatusOK, data
}
