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
	var importModules = map[string]string{}

	inPackageComponentTypes := indexComponentPackageTypes(component, "state")
	for _, def := range component.View.TypeDefinitions() {
		if inPackageComponentTypes[def.Name] {
			continue
		}
		if def.Package != "" && component.ModulePath != "" && strings.Contains(def.DataType, " ") { //complex type
			importModules[def.Package] = component.ModulePath
		}
		packageTypes = append(packageTypes, xreflect.NewType(def.Name, xreflect.WithPackage(def.Package), xreflect.WithTypeDefinition(def.DataType)))
	}
	inputState := xreflect.GenerateStruct("Input", input.Type(),
		xreflect.WithPackage("state"),
		xreflect.WithTypes(xreflect.NewType("Output", xreflect.WithReflectType(output))),
		xreflect.WithPackageTypes(packageTypes...),
		xreflect.WithRewriteDoc(),
		xreflect.WithImportModule(importModules),
	)

	builder.WriteString(inputState)
	result := builder.String()
	result = component.View.Resource().ReverseSubstitutes(result)
	return http.StatusOK, []byte(result)
}

func indexComponentPackageTypes(component *repository.Component, inPkg string) map[string]bool {
	thisPackageTypes := map[string]bool{}
	for _, def := range component.View.TypeDefinitions() {
		if def.Package == inPkg {
			thisPackageTypes[def.Name] = true
		}
	}
	return thisPackageTypes
}
