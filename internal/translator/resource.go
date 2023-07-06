package translator

import (
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox"
	"strings"
)

type Resource struct {
	Resource view.Resource
	State    inference.State
	Rule     *Rule
	parser.Statements
}

//ExtractDeclared extract both parameter declaration and transform expression
func (v *Resource) ExtractDeclared(dSQL *string) error {
	v.appendPathVariableParams()
	declarations, err := parser.NewDeclarations(*dSQL)
	if err != nil {
		return err
	}
	v.State.Append(declarations.State...)
	v.Rule.Route.Transforms = declarations.Transforms
	*dSQL = declarations.SQL
	return nil
}

func (v *Resource) appendPathVariableParams() {
	params := extractURIParams(v.Rule.Route.URI)
	for paramName := range params {
		v.State.Append(inference.NewPathParameter(paramName))
	}
}

func (v *Resource) InitRule(dSQL *string) error {
	if err := v.extractRuleSetting(dSQL); err != nil {
		return err
	}
	v.Statements = parser.NewStatements(*dSQL)
	route := &v.Rule.Route
	if route.Method == "" {
		route.Method = "GET"
	}

	//v.Rule.Route = &router.Route{
	//	Method:           method,
	//	EnableAudit:      true,
	//	CustomValidation: builder.option.CustomValidation || builder.option.HandlerType != "",
	//	Cors: &router.Cors{
	//		AllowCredentials: boolPtr(true),
	//		AllowHeaders:     stringsPtr("*"),
	//		AllowMethods:     stringsPtr("*"),
	//		AllowOrigins:     stringsPtr("*"),
	//		ExposeHeaders:    stringsPtr("*"),
	//	},
	//	URI:   combineURLs(s.config.APIPrefix, s.options.RoutePrefix, builder.session.routePrefix, builder.option.URI),
	//	Index: router.Index{Namespace: map[string]string{}},
	//	Output: router.Output{
	//		CaseFormat: "lc",
	//	},
	//}
	return nil
}

func (v *Resource) extractRuleSetting(dSQL *string) error {
	if index := strings.Index(*dSQL, "*/"); index != -1 {
		if err := parser.TryUnmarshalHint((*dSQL)[:index+2], &v.Rule.Route); err != nil {
			return err
		}
		*dSQL = (*dSQL)[index+2:]
	}
	return nil
}

func NewResource() *Resource {
	return &Resource{Rule: NewRule()}
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
