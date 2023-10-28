package gateway

import (
	"context"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/xreflect"
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
	statusCode, content := r.generateComponentState(component)
	setContentType(response, statusCode, "text/plain")
	write(response, statusCode, content)
}

func (r *Router) generateComponentState(component *repository.Component) (int, []byte) {
	builder := strings.Builder{}

	input := component.Input.Type.Type()
	registry := component.TypeRegistry()
	output, _ := component.Output.Type.Parameters.ReflectType("", registry.Lookup, false)

	inputState := xreflect.GenerateStruct("Input", input.Type(), xreflect.WithTypes(xreflect.NewType("Output", xreflect.WithReflectType(output))), xreflect.WithPackage("state"))
	builder.WriteString(inputState)
	return http.StatusOK, []byte(builder.String())
}
