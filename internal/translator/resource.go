package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/config"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/msg"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	expand2 "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx"
	"github.com/viant/toolbox"
	"github.com/viant/xreflect"
	"path"
	"reflect"
	"strings"
)

type (
	Resource struct {
		Generated  bool
		repository *options.Repository
		rule       *options.Rule
		Resource   view.Resource

		State       inference.State
		OutputState inference.State
		Rule        *Rule
		parser.Statements
		RawSQL string
		indexNamespaces
		UseCustomTypes bool
		Declarations   *parser.Declarations
		CustomTypeURLs []string
		typeRegistry   *xreflect.Types
		messages       *msg.Messages
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

func (r *Resource) GetSchema(dataType string, opts ...xreflect.Option) (*state.Schema, error) {
	registry := r.ensureRegistry()
	rType, err := registry.Lookup(dataType, opts...)
	if err != nil {
		return nil, err
	}
	schema := state.NewSchema(rType)

	if methods, _ := r.typeRegistry.Methods(dataType); len(methods) > 0 {
		schema.Methods = methods

	}
	if pkgSymbol, err := r.typeRegistry.Symbol("PackageName"); err == nil {
		if text, ok := pkgSymbol.(string); ok {
			schema.Package = text
		}
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
		if rType := schema.Type(); rType != nil {
			dataType = rType.String()
		}
		typeDef := &view.TypeDefinition{Name: name, Package: schema.Package, DataType: dataType, CustomType: len(schema.Methods) > 0}
		if i > 0 {
			alias = ""
		}
		//_ = config.Config.Types.Register(name, xreflect.WithTypeDefinition(dataType))
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
	r.OutputState.Append(r.Declarations.OutputState...)
	if len(r.State.FilterByKind(state.KindGroup)) > 0 {
		r.State = r.State.Group()
	}
	if r.State, err = r.State.Repeated(); err != nil {
		return err
	}
	if r.OutputState, err = r.OutputState.Repeated(); err != nil {
		return err
	}
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
	for _, parameter := range r.State.FilterByKind(state.KindDataView) {
		viewlet := NewViewlet(parameter.Name, parameter.SQL, nil, r)
		if parameter.Connector != "" {
			viewlet.Connector = parameter.Connector
		}
		viewlet.View.Mode = view.ModeQuery
		viewlet.View.ParameterDerived = true
		r.Rule.Viewlets.Append(viewlet)
	}
}

func (r *Resource) ImpliedKind() state.Kind {
	switch strings.ToLower(r.Rule.Method) {
	case "get":
		return state.KindQuery
	}
	return state.KindRequestBody
}

func (r *Resource) InitRule(dSQL *string, ctx context.Context, fs afs.Service, opts *options.Options) error {
	if err := r.extractRuleSetting(dSQL); err != nil {
		return err
	}
	r.Rule.IsGeneratation = opts.Generate != nil && opts.Generate.Operation != ""
	if opts != nil && r.Rule.IsGeneratation {
		r.Rule.Method = strings.ToUpper(opts.Generate.Operation)
	}
	if r.Rule.Output != nil {
		r.Rule.Route.Output = *r.Rule.Output
		r.Rule.Output = &r.Rule.Route.Output
	}
	r.Statements = parser.NewStatements(*dSQL)
	r.RawSQL = *dSQL
	return r.initRule(ctx, fs, dSQL)
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
	var bindingArgs []interface{}
	var options []expand2.StateOption
	epxandingSQL := viewlet.SanitizedSQL

	if metaViewSQL != nil {
		sourceViewName := metaViewSQL.Name[5 : len(metaViewSQL.Name)-4]
		epxandingSQL = strings.Replace(epxandingSQL, "$"+metaViewSQL.Name, "$View.NonWindowSQL", 1)
		sourceView := r.Rule.Viewlets.Lookup(sourceViewName)
		options = append(options, expand2.WithViewParam(&expand2.MetaParam{NonWindowSQL: sourceView.Expanded.Query, Args: sourceView.Expanded.Args, Limit: 1}))
		bindingArgs = sourceView.Expanded.Args
		viewlet.sourceViewlet = sourceView
		sourceView.View.EnsureTemplate()
		sourceView.View.Template.Summary = &view.TemplateSummary{ //TODO go for detail existing impl
			Source: epxandingSQL,
			Name:   viewlet.Name,
			Kind:   "record",
		}
	}

	epxandingSQL = viewlet.Resource.State.Expand(epxandingSQL)
	templateParameters := sqlState.ViewParameters()
	if strings.Contains(epxandingSQL, "$View.ParentJoinOn") {
		//TODO adjust parameter value type
		options = append(options, expand2.WithViewParam(&expand2.MetaParam{ParentValues: []interface{}{0}, DataUnit: &expand2.DataUnit{}}))
	}
	return viewlet.View.BuildParametrizedSQL(templateParameters, types, epxandingSQL, bindingArgs, options...)
}

func (r *Resource) ensureViewParametersSchema(ctx context.Context, setType func(ctx context.Context, setType *Viewlet) error) error {
	viewParameters := r.State.FilterByKind(state.KindDataView)
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
	parameters := r.State.FilterByKind(state.KindParam)
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

func (r *Resource) IncludeSnippets(ctx context.Context, fs afs.Service, dSQL *string) error {
	for _, URL := range r.Rule.Include {
		assetURL, err := r.IncludeURL(ctx, URL, fs)
		if err != nil {
			return err
		}

		content, err := fs.DownloadWithURL(ctx, assetURL)
		if err != nil {
			return err
		}

		ext := path.Ext(assetURL)
		switch ext {
		case ".sql", ".sqlx":
			contentStr := string(content)
			*dSQL = contentStr + "\n" + *dSQL

		case ".yaml", ".yml":
			resource := &view.Resource{}
			if err := shared.UnmarshalWithExt(content, resource, ext); err != nil {
				return err
			}

			(&r.Resource).MergeFrom(resource, nil)
		}

	}

	return nil
}

func (r *Resource) IncludeURL(ctx context.Context, URL string, fs afs.Service) (string, error) {
	return r.assetURL(ctx, URL, fs)
}

func NewResource(rule *options.Rule, repository *options.Repository, messages *msg.Messages) *Resource {
	ret := &Resource{Rule: NewRule(), rule: rule, repository: repository, messages: messages}
	ret.Rule.Output = &ret.Rule.Route.Output
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
