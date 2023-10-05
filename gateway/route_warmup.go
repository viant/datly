package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/view"
	"net/http"
)

func (r *Router) NewWarmupRoute(URL string, providers ...*repository.Provider) *Route {
	return &Route{
		Path:      component.NewPath(http.MethodPost, URL),
		Providers: providers,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleCacheWarmup(ctx, response, providers)
		},
		Kind: RouteWarmupKind,
		NewMultiRoute: func(routes []*component.Path) *Route {
			return r.NewWarmupRoute("", providers...)
		},
	}
}

func (r *Router) handleCacheWarmup(ctx context.Context, writer http.ResponseWriter, provider []*repository.Provider) {
	statusCode, content := r.handleCacheWarmupWithErr(ctx, provider)
	write(writer, statusCode, content)
}

func (r *Router) handleCacheWarmupWithErr(ctx context.Context, providers []*repository.Provider) (int, []byte) {
	var views []*view.View
	URIs := make([]string, len(providers))
	for i, provider := range providers {
		aComponent, err := provider.Component(ctx)
		if err != nil {
			return http.StatusInternalServerError, []byte(err.Error())
		}
		views, err := router.ExtractCacheableViews(ctx, aComponent)
		if err != nil {
			return http.StatusInternalServerError, []byte(err.Error())
		}
		views = append(views, views...)
		URIs[i] = aComponent.URI
	}

	lookup := func(_ context.Context, _, _ string) ([]*view.View, error) {
		return views, nil
	}
	response := warmup.PreCache(ctx, lookup, URIs...)
	data, err := json.Marshal(response)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}
	return http.StatusOK, data
}
