package translator

import (
	"fmt"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/config"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx"
	"github.com/viant/toolbox"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

type (
	Resource struct {
		rule     *options.Rule
		Resource view.Resource
		State    inference.State
		Rule     *Rule
		parser.Statements
		indexNamespaces
	}
)

// ExtractDeclared extract both parameter declaration and transform expression
func (r *Resource) ExtractDeclared(dSQL *string) error {
	r.appendPathVariableParams()
	declarations, err := parser.NewDeclarations(*dSQL)
	if err != nil {
		return err
	}
	r.State.Append(declarations.State...)
	r.Rule.Route.Transforms = declarations.Transforms
	*dSQL = declarations.SQL
	return nil
}

func (r *Resource) appendPathVariableParams() {
	params := extractURIParams(r.Rule.Route.URI)
	for paramName := range params {
		r.State.Append(inference.NewPathParameter(paramName))
	}
}

func (r *Resource) ImpliedKind() view.Kind {
	switch strings.ToLower(r.Rule.Method) {
	case "get":
		return view.KindQuery
	}
	return view.KindRequestBody
}

func (r *Resource) InitRule(dSQL *string) error {
	if err := r.extractRuleSetting(dSQL); err != nil {
		return err
	}
	r.Statements = parser.NewStatements(*dSQL)
	r.initRule()

	return nil
}

func (r *Resource) extractRuleSetting(dSQL *string) error {
	if index := strings.Index(*dSQL, "*/"); index != -1 {
		if err := parser.TryUnmarshalHint((*dSQL)[:index+2], &r.Rule.Route); err != nil {
			return err
		}
		*dSQL = (*dSQL)[index+2:]
	}
	return nil
}

func (r *Resource) expandSQL(n *Namespace) (*sqlx.SQL, error) {
	types := n.Resource.Resource.TypeRegistry()
	//TODO change with existing state build
	reflectType, err := n.Resource.State.ReflectType("autogen", types.Lookup)
	if err != nil {
		return nil, fmt.Errorf("failed to create state %v type: %w", n.Name, err)
	}
	parameters := n.Resource.State.ViewParameters()
	evaluator, err := view.NewEvaluator(parameters, reflectType, nil, n.SanitizedSQL, types.Lookup)
	if err != nil {
		return nil, fmt.Errorf("failed to create evaluator %v: %w", n.Name, err)
	}
	state := reflect.New(reflectType).Elem().Interface()
	result, err := evaluator.Evaluate(nil, expand.WithParameters(state, nil))
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate %v: %w", n.Name, err)
	}
	return &sqlx.SQL{Query: result.Expanded, Args: result.Context.DataUnit.ParamsGroup}, nil
}

func NewResource(rule *options.Rule) *Resource {
	ret := &Resource{Rule: NewRule(), rule: rule}
	ret.Resource.SetTypes(xreflect.NewTypes(
		xreflect.WithRegistry(config.Config.Types),
		xreflect.WithPackagePath(rule.Module)))
	return ret
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
