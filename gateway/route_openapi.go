package gateway

import (
	"context"
	"github.com/viant/datly/gateway/router/openapi"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"gopkg.in/yaml.v3"
	"net/http"
)

func (r *Router) NewOpenAPIRoute(URL string, components *repository.Service, providers ...*repository.Provider) *Route {
	return &Route{
		Path:      contract.NewPath(http.MethodGet, URL),
		Providers: providers,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleOpenAPI(ctx, components, response, req, providers)
		},
		Kind: RouteOpenAPIKind,
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
