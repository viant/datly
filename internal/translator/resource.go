package translator

import (
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox"
	"strings"
)

type Resource struct {
	Resource view.Resource
	State    inference.State
	Rule     *Rule
}

func (v *Resource) ExtractExplicitParameter(dSQL *string) error {
	v.appendPathVariableParams()
	if err := v.appendParameterDeclarationParams(dSQL); err != nil {
		return err
	}
	//	v.Resource.Parameters = TODO
	return nil
}

func (v *Resource) appendPathVariableParams() {
	params := extractURIParams(v.Rule.Route.URI)
	for paramName := range params {
		v.State.Append(&inference.Parameter{
			Parameter: view.Parameter{
				Name: paramName,
				In: &view.Location{
					Kind: view.KindPath,
					Name: paramName,
				},
			},
		})
	}
}

func (v *Resource) ExtractRouterOptions(dSQL *string) error {
	return nil
}

func NewResource() *Resource {
	return &Resource{Rule: &Rule{Route: &router.Route{}}}
}

func extractURIParams(URI string) map[string]bool {
	result := map[string]bool{}

	if URI == "" {
		return result
	}

	uriParams, _ := toolbox.ExtractURIParameters(URI, strings.NewReplacer("{", "", "}", "").Replace(URI))
	for _, param := range uriParams {
		result[param] = true
	}

	return result
}
