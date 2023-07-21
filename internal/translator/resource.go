package translator

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/config"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/internal/setter"
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
		Generated  bool
		repository *options.Repository
		rule       *options.Rule
		Resource   view.Resource

		State inference.State
		Rule  *Rule
		parser.Statements
		RawSQL string
		indexNamespaces
		UseCustomTypes bool
		Declarations   *parser.Declarations
		CustomTypeURLs []string
		typeRegistry   *xreflect.Types
	}
)

func (r *Resource) AddCustomTypeURL(URL string) {
	for _, candidate := range r.CustomTypeURLs {
		if candidate == URL {
			return
		}
	}
	r.CustomTypeURLs = append(r.CustomTypeURLs, URL)
}

func (r *Resource) GetSchema(dataType string, opts ...xreflect.Option) (*view.Schema, error) {
	registry := r.ensureRegistry()
	rType, err := registry.Lookup(dataType, opts...)
	if err != nil {
		return nil, err
	}
	schema := view.NewSchema(rType)
	if pkgSymbol, err := r.typeRegistry.Symbol("PackageName"); err == nil {
		if text, ok := pkgSymbol.(string); ok {
			schema.Package = text
		}
	}
	if methods, _ := r.typeRegistry.Methods(dataType); len(methods) > 0 {
		schema.Methods = methods
	}
	if strings.HasPrefix(dataType, "*") {
		dataType = dataType[1:]
	}
	schema.DataType = dataType
	return schema, nil
}

func (r *Resource) ensureRegistry() *xreflect.Types {
	if r.typeRegistry != nil {
		return r.typeRegistry
	}
	r.typeRegistry = xreflect.NewTypes(xreflect.WithRegistry(config.Config.Types))
	return r.typeRegistry
}

func (r *Resource) parseImports(ctx context.Context, dSQL *string) (err error) {
	if r.Rule.TypeSrc != nil {
		if err = r.loadImportTypes(ctx, r.Rule.TypeSrc); err != nil {
			return err
		}
	}
	if err = parser.ParseImports(ctx, dSQL, r.loadImportTypes); err != nil {
		return fmt.Errorf("failed to parse import statement: %w", err)
	}
	return nil
}

func (r *Resource) loadImportTypes(ctx context.Context, typesImport *parser.TypeImport) error {
	typesImport.EnsureLocation(ctx, fs, r.rule.GoModuleLocation())
	alias := typesImport.Alias
	for i, name := range typesImport.Types {
		if typeDef := r.TypeDefinition(name); typeDef != nil {
			return nil
		}
		schema, err := r.GetSchema(name, xreflect.WithPackagePath(typesImport.URL))
		if err != nil {
			return fmt.Errorf("unable to include import type: %v,  %w", name, err)
		}
		if len(schema.Methods) > 0 {
			r.AddCustomTypeURL(typesImport.URL)
		}
		dataType := schema.DataType
		pkg := schema.Package
		schema.Package = ""
		if rType := schema.Type(); rType != nil {
			dataType = rType.String()
		}
		typeDef := &view.TypeDefinition{Name: name, Package: pkg, DataType: dataType, CustomType: len(schema.Methods) > 0}
		if i > 0 {
			alias = ""
		}
		setter.SetStringIfEmpty(&typeDef.Alias, alias)
		r.AppendTypeDefinition(typeDef)
	}
	return nil
}

func (r *Resource) TypeDefinition(name string) *view.TypeDefinition {
	if len(r.Resource.Types) == 0 {
		return nil
	}
	for _, candidate := range r.Resource.Types {
		if candidate.Name == name {
			return candidate
		}
	}
	return nil
}

func (r *Resource) AppendTypeDefinition(typeDef *view.TypeDefinition) {
	if r.TypeDefinition(typeDef.Name) != nil {
		return
	}
	definition := *typeDef
	r.Resource.Types = append(r.Resource.Types, &definition)
}

func (r *Resource) AdjustCustomType(info *plugin.Info) {
	//TODO work in progress
	for i := range r.Resource.Types {
		aType := r.Resource.Types[i]
		if aType.CustomType {
			aType.DataType = aType.Name
			aType.CustomType = false
		}
	}
}

// ExtractDeclared extract both parameter declaration and transform expression
func (r *Resource) ExtractDeclared(dSQL *string) (err error) {
	r.appendPathVariableParams()
	r.Declarations, err = parser.NewDeclarations(*dSQL, r.GetSchema)
	if err != nil {
		return err
	}
	r.State.Append(r.Declarations.State...)
	r.Rule.Route.Transforms = r.Declarations.Transforms
	if err := parser.ExtractParameterHints(r.Declarations.SQL, &r.State); err != nil {
		return err
	}
	r.Declarations.SQL = parser.RemoveParameterHints(r.Declarations.SQL, r.State)
	*dSQL = r.Declarations.SQL
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

func (r *Resource) buildParameterViews() {
	for _, parameter := range r.State.FilterByKind(view.KindDataView) {
		viewlet := NewViewlet(parameter.Name, parameter.SQL, nil, r)
		if parameter.Connector != "" {
			viewlet.Connector = parameter.Connector
		}
		viewlet.View.Mode = view.ModeQuery
		viewlet.View.ParameterDerived = true
		r.Rule.Viewlets.Append(viewlet)
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
	r.RawSQL = *dSQL
	r.initRule()
	return nil
}

func (r *Resource) extractRuleSetting(dSQL *string) error {
	if index := strings.Index(*dSQL, "*/"); index != -1 {
		if err := inference.TryUnmarshalHint((*dSQL)[:index+2], &r.Rule); err != nil {
			return err
		}
		*dSQL = (*dSQL)[index+2:]
	}
	r.Rule.applyShortHands()
	return nil
}

func (r *Resource) expandSQL(viewlet *Viewlet) (*sqlx.SQL, error) {
	types := viewlet.Resource.Resource.TypeRegistry()
	resourceState := viewlet.Resource.State
	_ = resourceState.EnsureReflectTypes(r.rule.GoModuleLocation())
	sqlState := viewlet.Resource.State.StateForSQL(viewlet.SQL, r.Rule.Root == viewlet.Name)
	metaViewSQL := sqlState.MetaViewSQL()
	compacted, err := sqlState.Compact(r.rule.Module)
	if err == nil && len(compacted) > 0 {
		sqlState = compacted
	}

	sqlState = sqlState.RemoveReserved()
	reflectType, err := sqlState.ReflectType("autogen", types.Lookup)
	if err != nil {
		return nil, fmt.Errorf("failed to create state %v type: %w", viewlet.Name, err)
	}
	state := reflect.New(reflectType).Elem().Interface()

	var bindingArgs []interface{}

	var options []expand.StateOption
	epxandingSQL := viewlet.SanitizedSQL
	if strings.Contains(epxandingSQL, "$View.ParentJoinOn") {
		//TODO adjust parameter value type
		options = append(options, expand.WithViewParam(&expand.MetaParam{ParentValues: []interface{}{0}, DataUnit: &expand.DataUnit{}}))
	}
	options = append(options, expand.WithParameters(state, nil))
	if metaViewSQL != nil {
		sourceViewName := metaViewSQL.Name[5 : len(metaViewSQL.Name)-4]
		epxandingSQL = strings.Replace(epxandingSQL, "$"+metaViewSQL.Name, "$View.NonWindowSQL", 1)
		sourceView := r.Rule.Viewlets.Lookup(sourceViewName)
		options = append(options, expand.WithViewParam(&expand.MetaParam{NonWindowSQL: sourceView.Expanded.Query, Args: sourceView.Expanded.Args, Limit: 1}))
		bindingArgs = sourceView.Expanded.Args
		viewlet.sourceViewlet = sourceView
		sourceView.View.EnsureTemplate()
		sourceView.View.Template.Meta = &view.TemplateMeta{ //TODO go for detail existing impl
			Source: epxandingSQL,
			Name:   viewlet.Name,
			Kind:   "record",
		}
	}

	parameters := viewlet.Resource.State.ViewParameters()
	evaluator, err := view.NewEvaluator(parameters, reflectType, nil, epxandingSQL, types.Lookup)
	if err != nil {
		return nil, fmt.Errorf("failed to create evaluator %v: %w", viewlet.Name, err)
	}
	result, err := evaluator.Evaluate(nil, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate %v: %w", viewlet.Name, err)
	}
	bindingArgs = append(bindingArgs, result.Context.DataUnit.ParamsGroup...)
	return &sqlx.SQL{Query: result.Expanded, Args: bindingArgs}, nil
}

func (r *Resource) ensureViewParametersSchema(ctx context.Context, setType func(ctx context.Context, setType *Viewlet) error) error {
	viewParameters := r.State.FilterByKind(view.KindDataView)
	for _, viewParameter := range viewParameters {
		if viewParameter.Schema != nil && viewParameter.Schema.Type() != nil {
			continue
		}
		viewParameter.EnsureSchema()
		aViewNamespace := r.Rule.Viewlets.Lookup(viewParameter.Name)
		if err := setType(ctx, aViewNamespace); err != nil {
			return err
		}
		fields := aViewNamespace.Spec.Type.Fields()
		if len(fields) > 0 {
			paramSchema := reflect.StructOf(fields)
			viewParameter.Schema.SetType(paramSchema)
			viewParameter.Schema.DataType = viewParameter.Name
		}
		aViewNamespace.TypeDefinition = aViewNamespace.Spec.TypeDefinition("", false)
		aViewNamespace.TypeDefinition.Cardinality = viewParameter.Schema.Cardinality
	}
	return nil
}

func (r *Resource) ensureViewParameterSchema(parameter *inference.Parameter) error {
	if parameter.Schema != nil && parameter.Schema.Type() != nil {
		return nil
	}
	aView := r.Rule.Viewlets.Lookup(parameter.Name)
	aView.Spec.Type.Fields()
	return nil
}

func (r *Resource) ensurePathParametersSchema(ctx context.Context) error {
	parameters := r.State.FilterByKind(view.KindParam)
	if len(parameters) == 0 {
		return nil
	}
	for _, parameter := range parameters {
		schema := parameter.Schema
		rType := schema.Type()
		if rType == nil {
			continue
		}
		r.AppendTypeDefinition(&view.TypeDefinition{Name: schema.DataType, DataType: rType.String()})
	}
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
