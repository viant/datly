package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/gateway/router/openapi"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"gopkg.in/yaml.v3"
	"net/http"
	"strings"
)

func (r *Router) NewOpenAPIRoute(URL string, components *repository.Service, providers ...*repository.Provider) *Route {
	return &Route{
		Path:      contract.NewPath(http.MethodGet, URL),
		Providers: providers,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleOpenAPI(ctx, components, response, req, providers)
		},
		Kind:    RouteOpenAPIKind,
		Config:  r.config.Logging,
		Version: r.config.Version,
		NewMultiRoute: func(routes []*contract.Path) *Route {
			return r.NewOpenAPIRoute("", components, providers...)
		},
	}
}

func (r *Router) handleOpenAPI(ctx context.Context, components *repository.Service, res http.ResponseWriter, request *http.Request, provider []*repository.Provider) {
	statusCode, content := r.generateOpenAPI(ctx, components, provider)
	setContentType(res, statusCode, "text/yaml")
	write(res, statusCode, content)
}

func (r *Router) generateOpenAPI(ctx context.Context, components *repository.Service, providers []*repository.Provider) (int, []byte) {
	spec, err := openapi.GenerateOpenAPI3Spec(ctx, components, r.OpenAPIInfo, providers...)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	specMarshal, err := yaml.Marshal(spec)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	return http.StatusOK, specMarshal
}

// NewOpenAPIAggregateRoute builds a route that serves a single OpenAPI 3.0.1 spec
// covering all supplied (public) providers. It defaults to JSON and can return YAML
// when the request asks for it via ?format=yaml or an Accept header containing yaml.
func (r *Router) NewOpenAPIAggregateRoute(URL string, components *repository.Service, providers ...*repository.Provider) *Route {
	return &Route{
		Path:      contract.NewPath(http.MethodGet, URL),
		Providers: providers,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleOpenAPIAggregate(ctx, components, response, req, providers)
		},
		Kind:    RouteOpenAPIKind,
		Config:  r.config.Logging,
		Version: r.config.Version,
		NewMultiRoute: func(routes []*contract.Path) *Route {
			return r.NewOpenAPIAggregateRoute("", components, providers...)
		},
	}
}

func (r *Router) handleOpenAPIAggregate(ctx context.Context, components *repository.Service, res http.ResponseWriter, request *http.Request, providers []*repository.Provider) {
	asYAML := wantsYAML(request)
	statusCode, content, contentType := r.generateOpenAPIWithFormat(ctx, components, providers, asYAML)
	setContentType(res, statusCode, contentType)
	write(res, statusCode, content)
}

func (r *Router) generateOpenAPIWithFormat(ctx context.Context, components *repository.Service, providers []*repository.Provider, asYAML bool) (int, []byte, string) {
	spec, err := openapi.GenerateOpenAPI3Spec(ctx, components, r.OpenAPIInfo, providers...)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error()), "text/plain"
	}

	if asYAML {
		specMarshal, err := yaml.Marshal(spec)
		if err != nil {
			return http.StatusInternalServerError, []byte(err.Error()), "text/plain"
		}
		return http.StatusOK, specMarshal, "text/yaml"
	}

	specMarshal, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error()), "text/plain"
	}
	return http.StatusOK, specMarshal, "application/json"
}

func wantsYAML(request *http.Request) bool {
	if request == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(request.URL.Query().Get("format"))) {
	case "yaml", "yml":
		return true
	case "json":
		return false
	}
	accept := strings.ToLower(request.Header.Get("Accept"))
	if strings.Contains(accept, "yaml") {
		return true
	}
	return false
}
