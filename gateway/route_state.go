package gateway

import (
	"context"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"net/http"
	"strings"
)

func (r *Router) NewStateRoute(URL string, provider *repository.Provider) *Route {
	return &Route{
		Path:      contract.NewPath(http.MethodGet, URL),
		Providers: []*repository.Provider{provider},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleComponentState(ctx, response, provider)
		},
	}
}

func (r *Router) handleComponentState(ctx context.Context, response http.ResponseWriter, provider *repository.Provider) {
	component, err := provider.Component(ctx)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	statusCode, content := r.generateGoStruct(component)
	setContentType(response, statusCode, "text/plain")
	write(response, statusCode, content)
}

func (r *Router) generateComponentState(component *repository.Component) (int, []byte) {
	builder := strings.Builder{}

	//schemaType := component.View.Schema.CompType()
	//for schemaType.Kind() == reflect.Ptr {
	//	schemaType = schemaType.Elem()
	//}
	//
	//structContent := xreflect.GenerateStruct("GeneratedStruct", schemaType)
	//

	return http.StatusOK, []byte(builder.String())
}
