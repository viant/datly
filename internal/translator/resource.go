package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/msg"
	"github.com/viant/datly/internal/setter"
	tparser "github.com/viant/datly/internal/translator/parser"
	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/sqlx"
	"github.com/viant/tagly/format/text"
	"github.com/viant/toolbox"
	"github.com/viant/xreflect"
	"golang.org/x/mod/modfile"
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
		AsyncState  inference.State
		Rule        *Rule
		tparser.Statements
		RawSQL string
		indexNamespaces
		UseCustomTypes bool
		Declarations   *tparser.Declarations
		CustomTypeURLs []string
		typeRegistry   *xreflect.Types
		messages       *msg.Messages
		Module         *modfile.Module
		ModuleLocation string
		typePackages   map[string]string
	}
)

func (r *Resource) TypePackage(name string) string {
	name = strings.Replace(name, "*", "", 1)
	return r.typePackages[name]
}

func (r *Resource) GetURI() string {
	if r.rule.ModulePrefix == "" {
		return r.Rule.URI
	}
	return r.rule.ModulePrefix + r.Rule.URI
}

// LookupTypeDef returns matched type definition
func (r *Resource) LookupTypeDef(typeName string) *view.TypeDefinition {
	for _, typeDef := range r.Resource.Types {
		if typeDef.Name == typeName {
			return typeDef
		}
	}
	return nil
}

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

	if pkg, ok := r.typePackages[state.RawComponentType(dataType)]; ok {
		opts = append(opts, xreflect.WithPackage(pkg))
	}

	aType := xreflect.NewType(dataType, opts...)
	rType, err := registry.LookupType(aType)
	if err != nil {
		return nil, err
	}

	schema := state.NewSchema(rType, state.WithSchemaPackage(aType.Package), state.WithModulePath(aType.ModulePath), state.WithSchemaMethods(aType.Methods))
	if strings.HasPrefix(dataType, "*") {
		dataType = dataType[1:]
	}
	schema.Package = aType.Package
	schema.Name = dataType
	return schema, nil
}

func (r *Resource) ensureRegistry() *xreflect.Types {
	if r.typeRegistry != nil {
		return r.typeRegistry
	}
	registry := extension.Config.Types
	r.typeRegistry = xreflect.NewTypes(xreflect.WithRegistry(registry))
	return r.typeRegistry
}

func (r *Resource) parseImports(ctx context.Context, dSQL *string) (err error) {
	if r.Rule.TypeSrc != nil {
		if err = r.loadImportTypes(ctx, r.Rule.TypeSrc); err != nil {
			return err
		}
	}
	if err = tparser.ParseImports(ctx, dSQL, r.loadImportTypes); err != nil {
		return fmt.Errorf("failed to parse import statement: %w", err)
	}
	return nil
}

func (r *Resource) loadImportTypes(ctx context.Context, typesImport *tparser.TypeImport) error {
	typesImport.EnsureLocation(ctx, fs, r.rule.SourceCodeLocation())
	alias := typesImport.Alias
	for i, name := range typesImport.Types {
		if typeDef := r.LookupTypeDef(name); typeDef != nil {
			return nil
		}

		schema, err := r.GetSchema(name, xreflect.WithPackagePath(typesImport.URL))
		if err != nil {
			return fmt.Errorf("unable to include import type: %v,  %w", name, err)
		}
		r.typePackages[name] = schema.Package
		if len(schema.Methods) > 0 {
			r.AddCustomTypeURL(typesImport.URL)
		}
		dataType := schema.DataType
		if rType := schema.Type(); rType != nil {
			dataType = rType.String()
		}
		typeDef := &view.TypeDefinition{Name: name, Package: schema.Package, DataType: dataType, ModulePath: schema.ModulePath, CustomType: len(schema.Methods) > 0}
		if i > 0 {
			alias = ""
		}
		setter.SetStringIfEmpty(&typeDef.Alias, alias)
		r.AppendTypeDefinition(typeDef)
	}
	return nil
}

func (r *Resource) addParameterType(param *state.Parameter) {
	r.addParameterSchemaType(param)
	if param.Output != nil && param.Output.Schema != nil {
		rType := param.Output.Schema.Type()
		if types.EnsureStruct(rType) == nil {
			return
		}
		schema := param.Output.Schema
		typeName := schema.TypeName()
		setter.SetStringIfEmpty(&typeName, state.SanitizeTypeName(param.Name))
		schema.Name = typeName
		pkg := r.rule.Package()
		aType := xreflect.NewType(typeName, xreflect.WithReflectType(rType), xreflect.WithPackage(pkg))
		r.AppendTypeDefinition(&view.TypeDefinition{Name: typeName, DataType: aType.Body(), Package: pkg})
	}
}

func (r *Resource) addParameterSchemaType(param *state.Parameter) {
	typeName := reflect.StructTag(param.Tag).Get(xreflect.TagTypeName)
	if param.Schema.IsNamed() {
		return
	}
	if rType := param.Schema.Type(); rType != nil && types.EnsureStruct(rType) != nil {
		setter.SetStringIfEmpty(&param.Schema.Package, r.rule.Package())
		setter.SetStringIfEmpty(&typeName, state.SanitizeTypeName(param.Schema.Name))
		setter.SetStringIfEmpty(&typeName, state.SanitizeTypeName(param.Name))
		param.Schema.Name = typeName
		aType := xreflect.NewType(typeName, xreflect.WithReflectType(rType), xreflect.WithPackage(param.Schema.Package))
		r.AppendTypeDefinition(&view.TypeDefinition{Name: typeName, DataType: aType.Body(), Package: param.Schema.Package})
	}

}

func (r *Resource) AppendTypeDefinition(typeDef *view.TypeDefinition) {
	if r.LookupTypeDef(typeDef.Name) != nil {
		return
	}
	definition := *typeDef
	r.Resource.Types = append(r.Resource.Types, &definition)
	if typeDef.Schema.IsNamed() {
		return
	}
	r.typeRegistry.Register(typeDef.Name, xreflect.WithTypeDefinition(typeDef.DataType), xreflect.WithModulePath(typeDef.ModulePath), xreflect.WithPackage(typeDef.Package))
}

func (r *Resource) AppendTypeDefinitions(typeDefs []*view.TypeDefinition) {
	for _, def := range typeDefs {
		r.AppendTypeDefinition(def)
	}
}

func (r *Resource) AdjustCustomType() {
	//TODO work in progress
	for i := range r.Resource.Types {
		aType := r.Resource.Types[i]
		if rType, err := r.typeRegistry.Lookup(aType.Name, xreflect.WithPackage(aType.Package)); err == nil {
			if rType.Name() != "" { //if type is register it's named type, ignore it's def
				aType.CustomType = false
				prefix := ""
				if rType.Kind() == reflect.Struct {
					prefix = "*"
				}
				aType.DataType = prefix + aType.Name
				continue
			}
		}

		if aType.CustomType {
			aType.DataType = aType.Name
			aType.CustomType = false
		}
	}
}

// ExtractDeclared extract both parameter declaration and transform expression
func (r *Resource) ExtractDeclared(dSQL *string) (err error) {
	r.Declarations, err = tparser.NewDeclarations(*dSQL, r.GetSchema)
	if err != nil {
		return err
	}
	r.State.Append(r.Declarations.State...)

	r.appendPathVariableParams()

	r.OutputState.Append(r.Declarations.OutputState...)
	r.Rule.OutputParameter = r.OutputState.GetOutputParameter()

	if r.State, err = r.State.NormalizeComposites(); err != nil {
		return fmt.Errorf("failed to normalize input state: %w", err)
	}

	if doc := r.Rule.Doc.Parameters; doc != nil {
		r.State.AddDescriptions(doc)
	}

	if r.OutputState, err = r.OutputState.NormalizeComposites(); err != nil {
		return fmt.Errorf("failed to normalize output state: %w", err)
	}

	if err = r.OutputState.AdjustOutput(); err != nil {
		return err
	}
	if len(r.Declarations.AsyncState) > 0 {
		r.AsyncState = r.Declarations.AsyncState
	}
	r.Rule.Route.Transforms = r.Declarations.Transforms
	if err := tparser.ExtractParameterHints(r.Declarations.SQL, &r.State); err != nil {
		return err
	}
	r.Declarations.SQL = tparser.RemoveParameterHints(r.Declarations.SQL, r.State)
	*dSQL = r.Declarations.SQL
	return nil
}

func (r *Resource) appendPathVariableParams() {
	params := extractURIParams(r.Rule.Route.URI)
	required := true
	for paramName := range params {
		if param := r.State.Parameters().LookupByLocation(state.KindPath, paramName); param != nil {
			param.Required = &required
			continue
		}
		parameter := inference.NewPathParameter(paramName)
		parameter.Required = &required
		r.State.Append(parameter)
	}
}

func (r *Resource) buildParameterViews() {
	for _, parameter := range r.State.FilterByKind(state.KindView) {
		viewlet := NewViewlet(parameter.In.Name, parameter.SQL, nil, r)
		if parameter.Connector != "" {
			viewlet.Connector = parameter.Connector
		}
		if parameter.Schema != nil && parameter.Schema.DataType != "" {
			viewlet.DataType = parameter.Schema.DataType
			parameter.Schema.Name = strings.Replace(parameter.Schema.DataType, "*", "", 1)
		}
		viewlet.View.Mode = view.ModeQuery
		viewlet.View.ParameterDerived = true
		r.Rule.Viewlets.Append(viewlet)
	}

}

func (r *Resource) ImpliedKind() state.Kind {
	switch strings.ToLower(r.Rule.Method) {
	case "get":
		return state.KindForm
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
	r.Statements = tparser.NewStatements(*dSQL)
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
	compacted, err := sqlState.Compact(r.rule.ModuleLocation)
	if err == nil && len(compacted) > 0 {
		sqlState = compacted
	}

	sqlState = sqlState.RemoveReserved()
	var bindingArgs []interface{}
	var options []expand.StateOption
	sourceSQL := viewlet.SanitizedSQL

	if metaViewSQL != nil {
		cFormat := text.DetectCaseFormat(viewlet.Name)
		if err == nil && cFormat != text.CaseFormatUpperCamel {
			viewlet.Name = cFormat.Format(viewlet.Name, text.CaseFormatUpperCamel)
		}

		sourceViewName := metaViewSQL.Name[5 : len(metaViewSQL.Name)-4]
		sourceSQL = strings.Replace(sourceSQL, "$"+metaViewSQL.Name, "$View.NonWindowSQL", 1)
		sourceView := r.Rule.Viewlets.Lookup(sourceViewName)
		options = append(options, expand.WithViewParam(&expand.ViewContext{NonWindowSQL: sourceView.Expanded.Query, Args: sourceView.Expanded.Args, Limit: 1}))
		bindingArgs = sourceView.Expanded.Args
		viewlet.sourceViewlet = sourceView
		sourceView.View.EnsureTemplate()
		sourceView.View.Template.Summary = &view.TemplateSummary{ //TODO go for detail existing impl
			Source: sourceSQL,
			Name:   viewlet.Name,
			Kind:   "record",
		}

		viewlet.IsSummary = true
		sourceView.Summary = viewlet
	}

	sourceSQL = viewlet.Resource.State.Expand(sourceSQL)
	templateParameters := sqlState.Parameters()
	if strings.Contains(sourceSQL, "$View.ParentJoinOn") {
		//TODO adjust parameter value type
		options = append(options, expand.WithViewParam(&expand.ViewContext{ParentValues: []interface{}{0}, DataUnit: &expand.DataUnit{}}))
	}
	return viewlet.View.BuildParametrizedSQL(templateParameters, types, sourceSQL, bindingArgs, options...)
}

func (r *Resource) ensureViewParametersSchema(ctx context.Context, setType func(ctx context.Context, setType *Viewlet, doc state.Documentation) error, aDoc state.Documentation) error {
	viewParameters := r.State.FilterByKind(state.KindView)
	for _, viewParameter := range viewParameters {
		if viewParameter.Schema != nil && viewParameter.Schema.Type() != nil {
			continue
		}
		viewParameter.EnsureSchema()
		aViewNamespace := r.Rule.Viewlets.Lookup(viewParameter.In.Name)
		if err := setType(ctx, aViewNamespace, aDoc); err != nil {
			return err
		}
		fields := aViewNamespace.Spec.Type.Fields()
		if len(fields) > 0 {
			paramSchema := reflect.StructOf(fields)
			viewParameter.Schema.SetType(paramSchema)
		}
		aViewNamespace.TypeDefinition = aViewNamespace.Spec.TypeDefinition("", false, r.Rule.Doc.Columns)
		//	aViewNamespace.TypeDefinition.Cardinality = viewParameter.Schema.Cardinality
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

func (r *Resource) ensurePathParametersSchema(ctx context.Context, parameters inference.State) error {
	kindParameters := r.State.FilterByKind(state.KindParam)
	if len(parameters) == 0 {
		return nil
	}
	for _, parameter := range kindParameters {
		schema := parameter.Schema
		rType := schema.Type()
		if rType == nil {
			if baseParameter := parameters.Lookup(parameter.In.Name); baseParameter != nil {
				if baseParameter.Schema != nil && baseParameter.Schema.Type() != nil {
					parameter.Schema = baseParameter.Schema.Clone()
				}
			}
			continue
		}
		r.AppendTypeDefinition(&view.TypeDefinition{Name: schema.DataType, DataType: rType.String()})
	}
	return nil
}

func (r *Resource) IncludeSnippets(ctx context.Context, fs afs.Service, dSQL *string) error {
	sqlXContent := ""
	for _, URL := range r.Rule.Include {
		assetURL, err := r.IncludeURL(ctx, URL, fs)
		if err != nil {
			return err
		}

		content, err := fs.DownloadWithURL(ctx, assetURL)
		if err != nil {
			return err
		}
		content = []byte(r.Resource.Substitutes.Replace(string(content)))
		ext := path.Ext(assetURL)
		switch ext {
		case ".sql", ".sqlx", ".dql", ".dqlx":
			contentStr := string(content)
			if sqlXContent != "" {
				sqlXContent += "\n"
			}
			sqlXContent += contentStr
		case ".yaml", ".yml":
			resource := &view.Resource{}
			if err = shared.UnmarshalWithExt(content, resource, ext); err != nil {
				return err
			}

			(&r.Resource).MergeFrom(resource, nil)
		case ".go":
			if err = r.loadState(ctx, assetURL); err != nil {
				return err
			}
		}

	}
	if sqlXContent != "" {
		*dSQL = sqlXContent + "\n" + *dSQL
	}

	return nil
}

func (r *Resource) IncludeURL(ctx context.Context, URL string, fs afs.Service) (string, error) {
	return r.assetURL(ctx, URL, fs)
}

func (r *Resource) loadType(dirTypes *xreflect.DirTypes, typeName string, aPath string, registered map[string]map[string]bool, typeDefs *view.TypeDefinitions) (reflect.Type, error) {
	rType, err := dirTypes.Type(typeName)
	statePackage := dirTypes.PackagePath(aPath)
	if err != nil {
		return nil, fmt.Errorf("invalid typename: %v, %w", typeName, err)
	}
	delete(registered, statePackage)
	err = r.registerDependencies(registered, dirTypes, typeDefs)
	if err != nil {
		return nil, err
	}
	r.registerType(typeName, statePackage, rType, false, typeDefs)
	return rType, err
}

func (r *Resource) registerDependencies(registered map[string]map[string]bool, dirTypes *xreflect.DirTypes, defs *view.TypeDefinitions) error {
	for depPkg := range registered {
		packageDir := dirTypes.DirTypes(depPkg)
		typeNames := dirTypes.TypesInPackage(depPkg)
		for _, candidate := range typeNames {
			if registered[depPkg][candidate] {
				continue
			}
			rType, err := packageDir.Type(candidate)
			if err != nil {
				return err
			}
			r.registerType(candidate, depPkg, rType, false, defs)
		}
	}
	return nil
}

func (r *Resource) registerType(typeName string, pkg string, rType reflect.Type, customType bool, dest *view.TypeDefinitions) {
	if types.EnsureStruct(rType) == nil { //register only struct based type
		return
	}
	dataType := rType.Name()
	if dataType == "" {
		aType := xreflect.NewType(typeName, xreflect.WithPackage(pkg), xreflect.WithReflectType(rType))
		dataType = aType.Body()
		customType = len(aType.Methods) > 0
	}
	if dataType == "" {
		dataType = state.SanitizeTypeName(typeName)
	}
	*dest = append(*dest, &view.TypeDefinition{
		Name:       typeName,
		Package:    pkg,
		DataType:   dataType,
		CustomType: customType,
	})
}

func (r *Resource) extractState(loadType func(typeName string) (reflect.Type, error), rType reflect.Type, dest *inference.State) error {
	if rType.Kind() != reflect.Struct {
		return nil
	}
	for i := 0; i < rType.NumField(); i++ {
		aField := rType.Field(i)
		if _, ok := aField.Tag.Lookup(tags.ParameterTag); !ok {
			continue
		}
		parameter, err := state.BuildParameter(&aField, nil, nil)
		if err != nil {
			return err
		}
		iParameter := &inference.Parameter{Parameter: *parameter}
		parameter = &iParameter.Parameter
		if err = r.updateComposites(loadType, iParameter); err != nil {
			return err
		}

		iParameter.Explicit = true
		dest.Append(iParameter)
		if state.IsReservedAsyncState(iParameter.Name) {
			r.AsyncState.Append(iParameter)
		}
	}
	if len(r.AsyncState) == 1 && strings.ToLower(r.AsyncState[0].Name) == "userid" {
		r.AsyncState = inference.State{}
	}
	return nil
}

func (r *Resource) updateComposites(loadType func(typeName string) (reflect.Type, error), iParameter *inference.Parameter) (err error) {
	if err = r.updatedRepeated(loadType, iParameter); err != nil {
		return nil
	}
	if err = r.updatedObject(loadType, iParameter); err != nil {
		return nil
	}
	for _, anObject := range iParameter.Object {
		if err = r.updateComposites(loadType, anObject); err != nil {
			return err
		}
	}
	for _, anObject := range iParameter.Repeated {
		if err = r.updateComposites(loadType, anObject); err != nil {
			return err
		}
	}
	return err
}

func (r *Resource) updatedRepeated(loadType func(typeName string) (reflect.Type, error), iParameter *inference.Parameter) error {
	parameter := &iParameter.Parameter
	if parameter.In.Kind != state.KindRepeated {
		return nil
	}
	with := parameter.With
	if with == "" {
		return fmt.Errorf("with was empty, auxiliary type name is required for %v ypes ", parameter.In.Kind)
	}
	wType, err := loadType(with)
	if err != nil {
		return fmt.Errorf("failed to load parameter auxiliary type: %s, %w", with, err)
	}
	auxiliaryState := inference.State{}
	if err = r.extractState(loadType, wType, &auxiliaryState); err != nil {
		return fmt.Errorf("failed to extract parameters from auxiliary type: %s, %w", with, err)
	}
	for _, item := range auxiliaryState {
		parameter.Repeated = append(parameter.Repeated, &item.Parameter)
		iParameter.Repeated = append(iParameter.Repeated, item)
		if parameter.In.Name != "" {
			parameter.In.Name += ","
		}
		parameter.In.Name += item.Name
	}
	return nil
}

func (r *Resource) updatedObject(loadType func(typeName string) (reflect.Type, error), iParameter *inference.Parameter) error {

	parameter := &iParameter.Parameter
	if parameter.In.Kind != state.KindObject {
		return nil
	}

	iParameter.SyncObject()

	if len(iParameter.Object) > 0 {
		return nil
	}

	schema := parameter.OutputSchema()
	wType := schema.Type()
	if wType == nil {
		return fmt.Errorf("failed to get parameter auxiliary type: %s, %w", parameter.Name, schema.Name)
	}
	auxiliaryState := inference.State{}
	if err := r.extractState(loadType, wType, &auxiliaryState); err != nil {
		return fmt.Errorf("failed to extract parameters from auxiliary type: %s, %w", schema.Name, err)
	}

	for _, item := range auxiliaryState {
		parameter.Object = append(parameter.Object, &item.Parameter)
		iParameter.Object = append(iParameter.Object, item)
		if parameter.In.Name != "" {
			parameter.In.Name += ","
		}
		parameter.In.Name += item.Name
	}

	return nil
}

func NewResource(rule *options.Rule, repository *options.Repository, messages *msg.Messages) *Resource {
	ret := &Resource{Rule: NewRule(), rule: rule, repository: repository, messages: messages, typePackages: map[string]string{}}
	ret.typeRegistry = xreflect.NewTypes(xreflect.WithPackagePath(rule.ModuleLocation), xreflect.WithRegistry(extension.Config.Types))
	types := xreflect.NewTypes(
		xreflect.WithRegistry(ret.typeRegistry),
		xreflect.WithPackagePath(rule.ModuleLocation))
	ret.Resource.SetTypes(types)

	ret.Rule.Output = &ret.Rule.Route.Output

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
