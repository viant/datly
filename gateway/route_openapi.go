package gateway

import (
	"context"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/component"
	"gopkg.in/yaml.v3"
	"net/http"
)

func (r *Router) NewOpenAPIRoute(URL string, providers ...*repository.Provider) *Route {
	return &Route{
		Path:      component.NewPath(http.MethodGet, URL),
		Providers: providers,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleOpenAPI(ctx, response, req, providers)
		},
		Kind: RouteOpenAPIKind,
		NewMultiRoute: func(routes []*component.Path) *Route {
			return r.NewOpenAPIRoute("", providers...)
		},
	}
}

func (r *Router) handleOpenAPI(ctx context.Context, res http.ResponseWriter, request *http.Request, provider []*repository.Provider) {
	statusCode, content := r.generateOpenAPI(ctx, provider)
	setContentType(res, statusCode, "text/yaml")
	write(res, statusCode, content)
}

func (r *Router) generateOpenAPI(ctx context.Context, providers []*repository.Provider) (int, []byte) {
	spec, err := router.GenerateOpenAPI3Spec(ctx, r.OpenAPIInfo, providers...)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	specMarshal, err := yaml.Marshal(spec)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	return http.StatusOK, specMarshal
}
