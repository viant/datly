package gateway

import (
	"context"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/xreflect"
	"net/http"
	"reflect"
)

func (r *Router) NewStructRoute(URL string, provider *repository.Provider) *Route {
	return &Route{
		Path:      contract.NewPath(http.MethodGet, URL),
		Providers: []*repository.Provider{provider},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleGolangStruct(ctx, response, provider)
		},
		Config:  r.config.Logging,
		Version: r.config.Version,
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

	structContent := xreflect.GenerateStruct("GeneratedStruct", schemaType, xreflect.WithOnStructField(
		func(field *reflect.StructField, tag, typeName, documentation *string) {
			fieldTag := *tag
			fieldTag, _ = xreflect.RemoveTag(fieldTag, "on")
			fieldTag, _ = xreflect.RemoveTag(fieldTag, "sql")
			*tag = fieldTag
		}))
	return http.StatusOK, []byte(structContent)
}
