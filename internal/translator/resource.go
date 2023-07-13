package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
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
		repository *options.Repository
		rule       *options.Rule
		Resource   view.Resource
		State      inference.State
		Rule       *Rule
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
	required := true
	for paramName := range params {
		parameter := inference.NewPathParameter(paramName)
		parameter.Required = &required
		r.State.Append(parameter)
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
	r.Rule.Route.URI = url.Join(r.repository.APIPrefix, r.rule.Prefix, r.Rule.Route.URI)
	return nil
}

func (r *Resource) expandSQL(n *Namespace) (*sqlx.SQL, error) {
	types := n.Resource.Resource.TypeRegistry()

	sqlState := n.Resource.State.StateForSQL(n.SQL, r.Rule.Root == n.Name)
	reflectType, err := sqlState.ReflectType("autogen", types.Lookup)
	if err != nil {
		return nil, fmt.Errorf("failed to create state %v type: %w", n.Name, err)
	}
	state := reflect.New(reflectType).Elem().Interface()
	fmt.Printf("STA %T %+v %s\n", state, state, n.SanitizedSQL)

	parameters := n.Resource.State.ViewParameters()
	evaluator, err := view.NewEvaluator(parameters, reflectType, nil, n.SanitizedSQL, types.Lookup)
	if err != nil {
		return nil, fmt.Errorf("failed to create evaluator %v: %w", n.Name, err)
	}
	result, err := evaluator.Evaluate(nil, expand.WithParameters(state, nil))
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate %v: %w", n.Name, err)
	}
	return &sqlx.SQL{Query: result.Expanded, Args: result.Context.DataUnit.ParamsGroup}, nil
}

func (r *Resource) ensureViewParametersSchema(ctx context.Context, setType func(ctx context.Context, setType *Namespace) error) error {
	viewParameters := r.State.FilterByKind(view.KindDataView)
	for _, viewParameter := range viewParameters {
		if viewParameter.Schema != nil && viewParameter.Schema.Type() != nil {
			continue
		}
		viewParameter.EnsureSchema()
		aViewNamespace := r.Rule.Namespaces.Lookup(viewParameter.Name)
		if err := setType(ctx, aViewNamespace); err != nil {
			return err
		}
		fields := aViewNamespace.Spec.Type.Fields()
		if len(fields) > 0 {
			paramSchema := reflect.TypeOf(fields)
			viewParameter.Schema.SetType(paramSchema)
			viewParameter.Schema.DataType = viewParameter.Name
			viewParameter.Schema.Cardinality = view.One
		}
		aViewNamespace.TypeDefinition = aViewNamespace.Spec.TypeDefinition("")
	}
	return nil
}

func (r *Resource) ensureViewParameterSchema(parameter *inference.Parameter) error {
	if parameter.Schema != nil && parameter.Schema.Type() != nil {
		return nil
	}
	aView := r.Rule.Namespaces.Lookup(parameter.Name)
	aView.Spec.Type.Fields()
	fmt.Printf("11\n")
	return nil
}

func NewResource(rule *options.Rule, repository *options.Repository) *Resource {
	ret := &Resource{Rule: NewRule(), rule: rule, repository: repository}
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
