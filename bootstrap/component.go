package bootstrap

import (
	"fmt"
	routecompiler "github.com/viant/datly/bootstrap/routes"
	"github.com/viant/xdatly/response"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

// ContractResolver projects canonical linked or synthetic contract types into
// one component. Existing component metadata retains authority.
type ContractResolver struct {
	Component  *spec.Component
	InputType  *x.Type
	OutputType *x.Type
	Types      *typecatalog.Resolver
}

func (r ContractResolver) Resolve() (*spec.Component, error) {
	component := r.Component.Clone()
	if component == nil {
		component = &spec.Component{}
	}
	lookup := xshape.Lookup(nil)
	if r.Types != nil {
		lookup = r.Types.Descriptor
	}
	inputFields, err := xshape.New(r.InputType, lookup).Fields()
	if err != nil {
		return nil, fmt.Errorf("package input: %w", err)
	}
	var outputFields []xshape.Field
	// The public transport interface has no structural output fields. Its body
	// belongs to the handler; static documentation is compiled separately.
	if r.OutputType == nil || r.OutputType.Type != reflect.TypeFor[response.Response]() {
		outputFields, err = xshape.New(r.OutputType, lookup).Fields()
	}
	if err != nil {
		return nil, fmt.Errorf("package output: %w", err)
	}
	if err = mergeDescriptorImports(component, r.InputType); err != nil {
		return nil, fmt.Errorf("package input imports: %w", err)
	}
	if err = mergeDescriptorImports(component, r.OutputType); err != nil {
		return nil, fmt.Errorf("package output imports: %w", err)
	}
	resolver := &packageComponentResolver{
		component: component,
		contracts: []packageContract{
			{role: inputContract, fields: inputFields},
			{role: outputContract, fields: outputFields},
		},
	}
	resolver.indexCanonicalMetadata()
	resolved, err := resolver.resolve()
	if err != nil {
		return nil, err
	}
	if err = (routecompiler.Compiler{Component: resolved}).Compile(); err != nil {
		return nil, err
	}
	return resolved, nil
}

// Resolve creates canonical component metadata from one discovered package
// holder and its linked input contract.
func (s *RouteSource) Resolve(inputType, outputType reflect.Type) (*spec.Component, error) {
	component, err := s.canonicalComponent()
	if err != nil {
		return nil, err
	}
	return (ContractResolver{
		Component: component, InputType: xshape.Linked(inputType).Descriptor(), OutputType: xshape.Linked(outputType).Descriptor(),
	}).Resolve()
}

func (s *RouteSource) canonicalComponent() (*spec.Component, error) {
	if s == nil {
		return nil, fmt.Errorf("package route source is required")
	}
	name := s.componentName()
	component := &spec.Component{
		Key:           spec.Key{Kind: spec.KindComponent, Scope: s.PackagePath, Name: name},
		Name:          name,
		Description:   s.Tag.Description,
		Documentation: s.Tag.Documentation.Clone(),
		Example:       s.Tag.Example,
		Routes: []*spec.Route{{
			Name: s.Tag.RouteName, Method: s.Tag.Method, Path: s.Tag.Path,
			Internal:   s.Tag.Internal,
			Marshaller: strings.TrimSpace(s.Tag.Marshaller), Handler: strings.TrimSpace(s.Tag.Handler),
			APIKeyHeader: strings.TrimSpace(s.Tag.APIKeyHeader), APIKeyValue: s.Tag.APIKeyValue,
		}},
	}
	component.Routes[0].MCP = make([]*spec.MCPExposure, len(s.Tag.MCP))
	for index, exposure := range s.Tag.MCP {
		component.Routes[0].MCP[index] = exposure.Clone()
	}
	inputContract, err := packageContractExpression("input", s.InputType, s.Tag.Input)
	if err != nil {
		return nil, err
	}
	outputContract, err := packageContractExpression("output", s.OutputType, s.Tag.Output)
	if err != nil {
		return nil, err
	}
	if len(s.Imports) > 0 {
		component.TypeContext = &spec.TypeContext{
			DefaultPackage: strings.TrimSpace(s.PackagePath),
			Imports:        append([]spec.ImportSpec(nil), s.Imports...),
		}
	}
	settings := &spec.Settings{
		DefaultConnector: strings.TrimSpace(s.Tag.Connector),
		InputType:        inputContract,
		OutputType:       outputContract,
	}
	s.Tag.Settings.Apply(settings)
	if s.Tag.Report || s.Tag.ReportCompose != nil || s.Tag.ReportMCPTool != nil || s.Tag.ReportLinkedInputType != "" || s.Tag.ReportDimensions != "" || s.Tag.ReportMeasures != "" ||
		s.Tag.ReportFilters != "" || s.Tag.ReportOrderBy != "" || s.Tag.ReportLimit != "" || s.Tag.ReportOffset != "" {
		settings.Report = &spec.ReportSettings{
			Compose:         s.Tag.ReportCompose.Clone(),
			Enabled:         s.Tag.Report,
			LinkedInputType: s.Tag.ReportLinkedInputType,
		}
		if s.Tag.ReportDimensions != "" || s.Tag.ReportMeasures != "" || s.Tag.ReportFilters != "" ||
			s.Tag.ReportOrderBy != "" || s.Tag.ReportLimit != "" || s.Tag.ReportOffset != "" {
			settings.Report.InputLayout = &spec.ReportInputLayout{
				Dimensions: s.Tag.ReportDimensions, Measures: s.Tag.ReportMeasures, Filters: s.Tag.ReportFilters,
				OrderBy: s.Tag.ReportOrderBy, Limit: s.Tag.ReportLimit, Offset: s.Tag.ReportOffset,
			}
		}
		if s.Tag.ReportMCPTool != nil {
			enabled := *s.Tag.ReportMCPTool
			settings.Report.MCPTool = &enabled
		}
	}
	if len(settings.MCPFolders) > 0 || settings.Mutation != "" || settings.SequenceStrategy != "" || settings.DefaultConnector != "" || settings.InputType != "" || settings.OutputType != "" || settings.Report != nil ||
		settings.Cache != nil || settings.CaseFormat != "" || settings.JSONMarshalType != "" ||
		settings.JSONUnmarshalType != "" || settings.XMLUnmarshalType != "" || settings.Format != "" || settings.DateFormat != "" || settings.Output != nil || settings.IgnoreEmptyQueryParameters != nil {
		component.Settings = settings
	}
	if viewName := strings.TrimSpace(s.Tag.View); viewName != "" {
		component.RootView = &spec.View{
			Key: spec.Key{Kind: spec.KindView, Scope: s.PackagePath, Name: viewName}, Name: viewName,
		}
	}
	return component, nil
}

func (s *RouteSource) componentName() string {
	if s == nil {
		return ""
	}
	if name := strings.TrimSpace(s.Tag.Name); name != "" {
		return name
	}
	return strings.TrimSpace(s.FieldName)
}

type packageComponentResolver struct {
	component     *spec.Component
	contracts     []packageContract
	canonical     []*spec.Parameter
	fieldsByParam map[*spec.Parameter]string
	viewsByName   map[string]*spec.View
}

type contractRole uint8

const (
	inputContract contractRole = iota
	outputContract
)

type packageContract struct {
	role          contractRole
	fields        []xshape.Field
	paramsByField map[string]*spec.Parameter
}

type resolvedContractField struct {
	field    xshape.Field
	metadata *dtag.Field
	param    *spec.Parameter
}

func (r contractRole) label() string {
	if r == outputContract {
		return "output"
	}
	return "input"
}

func (r contractRole) accepts(param *spec.Parameter) bool {
	return isOutputParam(param) == (r == outputContract)
}

func (r *packageComponentResolver) indexCanonicalMetadata() {
	for index := range r.contracts {
		r.contracts[index].paramsByField = map[string]*spec.Parameter{}
	}
	r.fieldsByParam = map[*spec.Parameter]string{}
	r.canonical = spec.EffectiveParameters(r.component.Parameters)
	r.viewsByName = make(map[string]*spec.View, len(r.component.Views)*2)
	for _, view := range r.component.Views {
		if view == nil {
			continue
		}
		if view.Name != "" {
			r.viewsByName[view.Name] = view
		}
		if view.Key.Name != "" {
			r.viewsByName[view.Key.Name] = view
		}
	}
}

func (r *packageComponentResolver) resolve() (*spec.Component, error) {
	for index := range r.contracts {
		contract := &r.contracts[index]
		for _, field := range contract.fields {
			resolved, err := r.resolveField(contract, field)
			if err != nil {
				return nil, err
			}
			if resolved == nil {
				continue
			}
			if contract.role == outputContract {
				err = r.applyOutput(resolved)
			} else {
				err = r.applyInput(resolved)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return r.component, nil
}

func (r *packageComponentResolver) resolveField(contract *packageContract, field xshape.Field) (*resolvedContractField, error) {
	if !field.Exported {
		return nil, nil
	}
	metadata, err := dtag.ParseField(field.StructField())
	if err != nil {
		return nil, fmt.Errorf("resolve package %s %s: %w", contract.role.label(), field.Name, err)
	}
	param := contract.paramsByField[field.Name]
	if param == nil {
		param, err = r.matchCanonicalParam(contract.role, field, metadata)
		if err != nil {
			return nil, err
		}
	}
	if param == nil && metadata.Binding != nil {
		param, err = r.param(contract.role, field, metadata)
		if err != nil {
			return nil, err
		}
		if !contract.role.accepts(param) {
			return &resolvedContractField{field: field, metadata: metadata}, nil
		}
		r.component.Parameters = append(r.component.Parameters, param)
	}
	if param != nil {
		contract.paramsByField[field.Name] = param
	}
	if err = r.resolveParamType(contract.role, field, param); err != nil {
		return nil, err
	}
	return &resolvedContractField{field: field, metadata: metadata, param: param}, nil
}

func (r *packageComponentResolver) resolveParamType(role contractRole, field xshape.Field, param *spec.Parameter) error {
	if param == nil || strings.TrimSpace(param.TypeExpr) != "" {
		return nil
	}
	if role == inputContract && param.Codec != nil && param.IsTransportInput() {
		return fmt.Errorf("resolve package input %s: codec source dataType is required", field.Name)
	}
	typeExpr, err := r.fieldTypeExpression(field)
	if err != nil {
		return fmt.Errorf("resolve package %s %s type: %w", role.label(), field.Name, err)
	}
	param.TypeExpr = typeExpr
	return nil
}

func (r *packageComponentResolver) applyInput(resolved *resolvedContractField) error {
	param := resolved.param
	if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "view") {
		return nil
	}
	name, err := r.viewName(resolved.field, param, resolved.metadata)
	if err != nil {
		return err
	}
	param.Source.Name = name
	if r.viewsByName[name] != nil {
		return nil
	}
	view, err := r.view(resolved.field, name, resolved.metadata)
	if err != nil {
		return err
	}
	r.component.Views = append(r.component.Views, view)
	r.viewsByName[name] = view
	return nil
}

func (r *packageComponentResolver) applyOutput(resolved *resolvedContractField) error {
	param := resolved.param
	if param.IsDerivedOutput() {
		return r.addOutputRelation(resolved.field, param, resolved.metadata)
	}
	if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") ||
		!strings.EqualFold(strings.TrimSpace(param.Source.Name), "view") {
		return nil
	}
	if r.component.RootView != nil && r.component.RootView.Source != nil {
		return nil
	}
	name, err := r.rootViewName(resolved.field, resolved.metadata)
	if err != nil {
		return err
	}
	view, err := r.view(resolved.field, name, resolved.metadata)
	if err != nil {
		return err
	}
	if r.component.RootView != nil {
		view.Columns = r.component.RootView.Columns
	}
	r.component.RootView = view
	return nil
}

func (r *packageComponentResolver) addOutputRelation(field xshape.Field, param *spec.Parameter, metadata *dtag.Field) error {
	if r.component.RootView == nil {
		return fmt.Errorf("resolve package output %s: derived view requires a root view", field.Name)
	}
	holder := strings.TrimSpace(param.Name)
	if holder == "" {
		holder = field.Name
	}
	var existing *spec.Relation
	for _, relation := range r.component.RootView.Relations {
		if relation == nil {
			continue
		}
		canonicalMatch := relationMatchesOutput(relation, holder)
		fieldCollision := equalMetadataName(relation.Holder, field.Name)
		if !canonicalMatch && !fieldCollision {
			continue
		}
		if !canonicalMatch {
			return fmt.Errorf("resolve package output %s: holder is already owned by relation %q", field.Name, relation.Name)
		}
		if relation.Kind != spec.RelationKindDerived {
			return fmt.Errorf("resolve package output %s: derived view %q conflicts with %s relation %q on holder %q",
				field.Name, holder, relation.Kind, relation.Name, relation.Holder)
		}
		if existing != nil && existing != relation {
			return fmt.Errorf("resolve package output %s: derived view %q is ambiguous", field.Name, holder)
		}
		existing = relation
	}
	if existing != nil {
		existing.Holder = field.Name
		return nil
	}
	view, err := r.view(field, holder, metadata)
	if err != nil {
		return err
	}
	view.Cardinality = spec.CardinalityOne
	r.component.RootView.Relations = append(r.component.RootView.Relations, &spec.Relation{
		Name: holder, Kind: spec.RelationKindDerived, Holder: field.Name,
		Cardinality: spec.CardinalityOne, View: view,
	})
	return nil
}

func relationMatchesOutput(relation *spec.Relation, logicalHolder string) bool {
	if relation == nil {
		return false
	}
	return equalMetadataName(relation.Name, logicalHolder) || equalMetadataName(relation.Holder, logicalHolder)
}

func equalMetadataName(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return left != "" && right != "" && strings.EqualFold(left, right)
}

func (r *packageComponentResolver) matchCanonicalParam(role contractRole, field xshape.Field, metadata *dtag.Field) (*spec.Parameter, error) {
	if metadata == nil || metadata.Binding == nil {
		return nil, nil
	}
	binding := metadata.Binding
	logicalName := strings.TrimSpace(binding.Name)
	if logicalName == "" {
		logicalName = field.Name
	}
	byName := r.canonicalCandidates(role, func(param *spec.Parameter) bool {
		return strings.EqualFold(strings.TrimSpace(param.Name), logicalName)
	})
	hasSource := strings.TrimSpace(binding.Location.Kind) != ""
	if hasSource {
		byName = filterCanonicalSource(byName, binding.Location.Kind, binding.Location.In)
	}
	if len(byName) == 0 && hasSource {
		byName = r.canonicalCandidates(role, func(param *spec.Parameter) bool {
			return sameCanonicalSource(param, binding.Location.Kind, binding.Location.In)
		})
	}
	if len(byName) == 0 {
		return nil, nil
	}
	if len(byName) > 1 {
		return nil, fmt.Errorf("resolve package field %s: canonical parameter match for %q is ambiguous", field.Name, logicalName)
	}
	param := byName[0]
	if assigned := r.fieldsByParam[param]; assigned != "" && assigned != field.Name {
		return nil, fmt.Errorf("resolve package field %s: canonical parameter %q is already mapped to field %s", field.Name, param.Name, assigned)
	}
	r.fieldsByParam[param] = field.Name
	return param, nil
}

func (r *packageComponentResolver) canonicalCandidates(role contractRole, matches func(*spec.Parameter) bool) []*spec.Parameter {
	var result []*spec.Parameter
	for _, param := range r.canonical {
		if param == nil || !role.accepts(param) || !matches(param) {
			continue
		}
		result = append(result, param)
	}
	return result
}

func filterCanonicalSource(params []*spec.Parameter, kind, name string) []*spec.Parameter {
	result := make([]*spec.Parameter, 0, len(params))
	for _, param := range params {
		if sameCanonicalSource(param, kind, name) {
			result = append(result, param)
		}
	}
	return result
}

func sameCanonicalSource(param *spec.Parameter, kind, name string) bool {
	return param != nil && strings.EqualFold(strings.TrimSpace(param.Source.Kind), strings.TrimSpace(kind)) &&
		strings.EqualFold(strings.TrimSpace(param.Source.Name), strings.TrimSpace(name))
}

func isOutputParam(param *spec.Parameter) bool {
	return param != nil && (param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output"))
}

func (r *packageComponentResolver) param(role contractRole, field xshape.Field, metadata *dtag.Field) (*spec.Parameter, error) {
	binding := metadata.Binding
	name := strings.TrimSpace(binding.Name)
	if name == "" {
		name = field.Name
	}
	param := &spec.Parameter{
		Name: name, Source: spec.BindSource{Kind: binding.Location.Kind, Name: binding.Location.In},
		TypeExpr: binding.DataType, Tag: string(field.Tag), Cardinality: binding.Cardinality,
		Required: binding.Required, Cacheable: binding.Cacheable,
		MinAllowedRecords: binding.MinAllowedRecords, MaxAllowedRecords: binding.MaxAllowedRecords, ExpectedReturned: binding.ExpectedReturned,
		MCP: metadata.MCP, PathMCP: metadata.PathMCP,
		When: binding.When, Scope: binding.Scope, With: binding.With,
		Activation: activationFromBinding(binding.URI), ResourceRef: resourceFromBinding(binding.URI, binding.ResourceRef), Async: binding.Async,
		ErrorStatusCode: binding.ErrorCode, ErrorMessage: binding.ErrorMessage,
	}
	if binding.DefaultValue != nil {
		value, ok := binding.DefaultValue.(string)
		if !ok {
			return nil, fmt.Errorf("resolve package %s %s: bind default must be a string, got %T", role.label(), field.Name, binding.DefaultValue)
		}
		param.Value = &value
	}
	if metadata.Codec != nil {
		param.Codec = &spec.Codec{
			Body: metadata.Codec.Body, Args: append([]string(nil), metadata.Codec.Arguments...), OutputType: metadata.Codec.OutputType,
		}
	}
	param.Description = metadata.Description
	param.Example = metadata.Example
	for _, predicate := range metadata.Predicates {
		if predicate == nil {
			continue
		}
		cloned := *predicate
		cloned.Args = append([]string(nil), predicate.Args...)
		param.Predicates = append(param.Predicates, &cloned)
	}
	if metadata.QuerySelector != nil {
		property, ok := spec.SelectorPropertyForParam(name)
		if !ok {
			return nil, fmt.Errorf("resolve package %s %s: query selector is not supported for parameter %q", role.label(), field.Name, name)
		}
		param.QuerySelector = &spec.QuerySelectorBinding{View: metadata.QuerySelector.View, Property: property}
	}
	return param, nil
}

func activationFromBinding(uri string) *spec.RouteActivation {
	uri = strings.TrimSpace(uri)
	if !strings.HasPrefix(uri, "/") {
		return nil
	}
	return &spec.RouteActivation{URI: uri}
}

func resourceFromBinding(uri, explicit string) string {
	if explicit = strings.TrimSpace(explicit); explicit != "" {
		return explicit
	}
	uri = strings.TrimSpace(uri)
	if strings.HasPrefix(uri, "/") {
		return ""
	}
	return uri
}

func (r *packageComponentResolver) viewName(field xshape.Field, param *spec.Parameter, metadata *dtag.Field) (string, error) {
	name := strings.TrimSpace(param.Source.Name)
	if name == "" {
		name = strings.TrimSpace(param.Name)
	}
	if name == "" {
		name = field.Name
	}
	if metadata.View == nil || strings.TrimSpace(metadata.View.Name) == "" {
		return name, nil
	}
	viewName := strings.TrimSpace(metadata.View.Name)
	if name != viewName {
		return "", fmt.Errorf("resolve package input %s: view binding %q conflicts with view tag %q", field.Name, name, viewName)
	}
	return name, nil
}

func (r *packageComponentResolver) rootViewName(field xshape.Field, metadata *dtag.Field) (string, error) {
	name := ""
	if r.component.RootView != nil {
		name = strings.TrimSpace(r.component.RootView.Name)
		if name == "" {
			name = strings.TrimSpace(r.component.RootView.Key.Name)
		}
	}
	tagName := ""
	if metadata.View != nil {
		tagName = strings.TrimSpace(metadata.View.Name)
	}
	if name != "" && tagName != "" && name != tagName {
		return "", fmt.Errorf("resolve package output %s: component view %q conflicts with view tag %q", field.Name, name, tagName)
	}
	if name == "" {
		name = tagName
	}
	if name == "" {
		name = field.Name
	}
	return name, nil
}

func (r *packageComponentResolver) view(field xshape.Field, name string, metadata *dtag.Field) (*spec.View, error) {
	if metadata.View == nil && metadata.SQL == nil {
		return nil, fmt.Errorf("resolve package input %s: view %q requires view or SQL metadata", field.Name, name)
	}
	view := &spec.View{
		Key:    spec.Key{Kind: spec.KindView, Scope: r.viewScope(field), Name: name},
		Name:   name,
		Source: &spec.ViewSource{},
	}
	if metadata.View != nil {
		view.TypeName = strings.TrimSpace(metadata.View.TypeName)
		view.Dest = strings.TrimSpace(metadata.View.Dest)
		view.EntityHooks = strings.TrimSpace(metadata.View.EntityHooks)
		view.Source.URI = strings.TrimSpace(metadata.View.URI)
		view.Source.Table = strings.TrimSpace(metadata.View.Table)
		view.Partitioning = metadata.View.Partitioning.Clone()
		view.Selector = metadata.View.Selector.Clone()
		view.BatchSize = metadata.View.Batch
		view.BatchConcurrency = metadata.View.BatchConcurrency
		view.Auxiliary = metadata.View.Auxiliary
		view.PublishParent = metadata.View.PublishParent
		view.RelationalConcurrency = metadata.View.RelationalConcurrency
		if metadata.View.AllowNulls != nil {
			value := *metadata.View.AllowNulls
			view.AllowNulls = &value
		}
		if metadata.View.Groupable != nil {
			value := *metadata.View.Groupable
			view.Groupable = &value
		}
		connector := strings.TrimSpace(metadata.View.Connector)
		cache := strings.TrimSpace(metadata.View.Cache)
		if connector != "" || cache != "" || strings.TrimSpace(metadata.View.CacheWarmup) != "" {
			view.Source.Bindings = &spec.ViewBindings{Connector: connector, CacheName: cache}
			view.Source.Bindings.CacheWarmup = strings.TrimSpace(metadata.View.CacheWarmup)
		}
		if metadata.View.Limit != nil || metadata.View.Offset != nil || strings.TrimSpace(metadata.View.OrderBy) != "" {
			view.Source.Controls = &spec.ViewControls{OrderBy: strings.TrimSpace(metadata.View.OrderBy)}
		}
		if metadata.View.Limit != nil {
			limit := *metadata.View.Limit
			view.Source.Controls.Limit = &limit
		}
		if metadata.View.Offset != nil {
			offset := *metadata.View.Offset
			view.Source.Controls.Offset = &offset
		}
	}
	if metadata.SQL != nil {
		view.Source.SQL = metadata.SQL.Text
		if metadata.SQL.URI != "" {
			view.Source.URI = metadata.SQL.URI
		}
	}
	if strings.TrimSpace(view.Source.Table) == "" && strings.TrimSpace(view.Source.SQL) == "" && strings.TrimSpace(view.Source.URI) == "" && len(view.Source.Embeds) == 0 {
		return nil, fmt.Errorf("resolve package input %s: view %q requires SQL or table source", field.Name, name)
	}
	return view, nil
}

func (r *packageComponentResolver) viewScope(field xshape.Field) string {
	if scope := strings.TrimSpace(r.component.Key.Scope); scope != "" {
		return scope
	}
	fieldType := field.ReflectedType
	if fieldType == nil {
		return ""
	}
	for fieldType.Kind() == reflect.Ptr || fieldType.Kind() == reflect.Slice || fieldType.Kind() == reflect.Array {
		fieldType = fieldType.Elem()
	}
	return fieldType.PkgPath()
}
