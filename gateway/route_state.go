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
	output, _ := component.Output.Type.Parameters.ReflectType("", registry.Lookup)

	var packageTypes []*xreflect.Type

	inPackageComponentTypes := indexComponentPackageTypes(component)
	for _, def := range component.View.TypeDefinitions() {
		if inPackageComponentTypes[def.Name] {
			continue
		}
		packageTypes = append(packageTypes, xreflect.NewType(def.Name, xreflect.WithPackage(def.Package), xreflect.WithTypeDefinition(def.DataType)))
	}
	inputState := xreflect.GenerateStruct("Input", input.Type(),
		xreflect.WithTypes(xreflect.NewType("Output", xreflect.WithReflectType(output))), xreflect.WithPackage("state"),
		xreflect.WithPackageTypes(packageTypes...),
	)
	builder.WriteString(inputState)
	return http.StatusOK, []byte(builder.String())
}

func indexComponentPackageTypes(component *repository.Component) map[string]bool {
	thisPackageTypes := map[string]bool{}
	pkg := component.Output.Type.Package
	for _, def := range component.View.TypeDefinitions() {
		if def.Package == pkg {
			thisPackageTypes[def.Name] = true
		}
	}
	return thisPackageTypes
}
