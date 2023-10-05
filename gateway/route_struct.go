package gateway

import (
	"context"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/component"
	"github.com/viant/xreflect"
	"net/http"
	"reflect"
)

func (r *Router) NewStructRoute(URL string, provider *repository.Provider) *Route {
	return &Route{
		Path:      component.NewPath(http.MethodGet, URL),
		Providers: []*repository.Provider{provider},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleGolangStruct(ctx, response, provider)
		},
	}
}

func (r *Router) handleGolangStruct(ctx context.Context, response http.ResponseWriter, provider *repository.Provider) {
	component, err := provider.Component(ctx)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	statusCode, content := r.generateGoStruct(component)
	setContentType(response, statusCode, "text/plain")
	write(response, statusCode, content)
}

func (r *Router) generateGoStruct(component *repository.Component) (int, []byte) {
	schemaType := component.View.Schema.CompType()
	for schemaType.Kind() == reflect.Ptr {
		schemaType = schemaType.Elem()
	}

	structContent := xreflect.GenerateStruct("GeneratedStruct", schemaType)
	return http.StatusOK, []byte(structContent)
}
