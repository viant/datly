package load

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/compile/pipeline"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	shapevalidate "github.com/viant/datly/repository/shape/validate"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/sqlparser"
	"github.com/viant/xdatly/handler/response"
)

// Loader materializes runtime view artifacts from normalized shape plan.
type Loader struct{}

// New returns shape loader implementation.
func New() *Loader {
	return &Loader{}
}

// LoadViews implements shape.Loader.
func (l *Loader) LoadViews(ctx context.Context, planned *shape.PlanResult, opts ...shape.LoadOption) (*shape.ViewArtifacts, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	loadOptions := &shape.LoadOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(loadOptions)
		}
	}
	pResult, resource, err := l.materialize(planned, loadOptions)
	if err != nil {
		return nil, err
	}
	if len(pResult.Views) == 0 {
		return nil, ErrEmptyViewPlan
	}
	return &shape.ViewArtifacts{Resource: resource, Views: resource.Views}, nil
}

// LoadComponent implements shape.Loader.
func (l *Loader) LoadComponent(ctx context.Context, planned *shape.PlanResult, opts ...shape.LoadOption) (*shape.ComponentArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	loadOptions := &shape.LoadOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(loadOptions)
		}
	}
	pResult, resource, err := l.materialize(planned, loadOptions)
	if err != nil {
		return nil, err
	}
	if err := validateComponentRoutes(pResult.Components); err != nil {
		return nil, err
	}
	if len(pResult.Views) == 0 {
		if err := materializeComponentRouteView(planned.Source, pResult, resource); err != nil {
			return nil, err
		}
		if len(resource.Views) == 0 && !allowsViewlessComponent(pResult.Components) {
			return nil, ErrEmptyViewPlan
		}
	}
	component := buildComponent(planned.Source, pResult, resource, loadOptions)
	return &shape.ComponentArtifact{
		Resource:  resource,
		Component: component,
	}, nil
}

func validateComponentRoutes(routes []*plan.ComponentRoute) error {
	count := 0
	for _, route := range routes {
		if route != nil {
			count++
		}
	}
	if count <= 1 {
		return nil
	}
	return fmt.Errorf("shape load: multiple component routes are not supported for a single component artifact")
}

func allowsViewlessComponent(routes []*plan.ComponentRoute) bool {
	for _, route := range routes {
		if route != nil {
			return true
		}
	}
	return false
}

func (l *Loader) materialize(planned *shape.PlanResult, loadOptions *shape.LoadOptions) (*plan.Result, *view.Resource, error) {
	if planned == nil || planned.Source == nil {
		return nil, nil, shape.ErrNilSource
	}
	pResult, ok := plan.ResultFrom(planned)
	if !ok {
		return nil, nil, fmt.Errorf("shape load: unsupported plan kind %q", planned.Plan.ShapeSpecKind())
	}
	resource := view.EmptyResource()
	if pResult.EmbedFS != nil {
		resource.SetFSEmbedder(state.NewFSEmbedder(pResult.EmbedFS))
	}
	for _, item := range pResult.Views {
		aView, err := materializeView(item)
		if err != nil {
			return nil, nil, err
		}
		if loadOptions != nil && loadOptions.UseTypeContextPackages {
			inheritViewSchemaPackage(aView, pResult.TypeContext)
		}
		resource.AddViews(aView)
	}
	attachViewRelations(resource, pResult.Views)
	if err := enrichRelationHolderTypes(resource, pResult.Views); err != nil {
		return nil, nil, err
	}
	rootView := rootResourceView(resource, pResult.Views)
	for _, item := range pResult.States {
		if item == nil {
			continue
		}
		param := cloneStateParameter(item)
		if param == nil {
			continue
		}
		normalizeDerivedInputSchema(param, resource)
		if rootView != nil {
			inheritRootOutputSchema(param, rootView)
		}
		ensureMaterializedOutputSchema(param, rootView)
		resource.AddParameters(param)
	}
	if err := shapevalidate.ValidateRelations(resource, resource.Views...); err != nil {
		return nil, nil, err
	}
	if len(pResult.Const) > 0 {
		for k, v := range pResult.Const {
			constParam := &state.Parameter{
				Name:  k,
				In:    state.NewConstLocation(k),
				Value: v,
				Tag:   `internal:"true"`,
				Schema: &state.Schema{
					Name:        "string",
					DataType:    "string",
					Cardinality: state.One,
				},
			}
			resource.AddParameters(constParam)
		}
	}
	bindTemplateParameters(resource)
	// Apply cache directives only as resource-level provider definitions.
	// View-level cache binding comes from explicit view metadata such as set_cache(...).
	if pResult.Directives != nil && pResult.Directives.Cache != nil {
		if name := strings.TrimSpace(pResult.Directives.Cache.Name); name != "" {
			provider := strings.TrimSpace(pResult.Directives.Cache.Provider)
			location := strings.TrimSpace(pResult.Directives.Cache.Location)
			ttlMs := pResult.Directives.Cache.TimeToLiveMs
			if provider != "" && location != "" && ttlMs > 0 {
				resource.CacheProviders = append(resource.CacheProviders, &view.Cache{
					Name:         name,
					Provider:     provider,
					Location:     location,
					TimeToLiveMs: ttlMs,
				})
			}
		}
	}
	return pResult, resource, nil
}

func buildComponent(source *shape.Source, pResult *plan.Result, resource *view.Resource, loadOptions *shape.LoadOptions) *Component {
	component := &Component{Method: "GET"}
	if source != nil {
		component.Name = source.Name
		component.URI = source.Name
	}
	component.TypeContext = cloneTypeContext(pResult.TypeContext)
	applyComponentRoutes(component, pResult.Components)
	applyViewMeta(component, pResult.Views)
	applyStateBuckets(component, pResult.States, resource, loadOptions)
	applyStateBuckets(component, synthesizeConstStates(pResult.Const), resource, loadOptions)
	applyStateBuckets(component, synthesizeMissingRouteContractStates(component, pResult.Components), resource, loadOptions)
	component.Input = append(component.Input, synthesizePredicateStates(component.Input, component.Predicates)...)
	component.Directives = cloneDirectives(pResult.Directives)
	component.ColumnsDiscovery = pResult.ColumnsDiscovery
	component.TypeSpecs = resolveTypeSpecs(pResult)
	return component
}

func applyComponentRoutes(component *Component, routes []*plan.ComponentRoute) {
	if component == nil || len(routes) == 0 {
		return
	}
	component.ComponentRoutes = cloneComponentRoutes(routes)
	primary := firstComponentRoute(routes)
	if primary == nil {
		return
	}
	if uri := strings.TrimSpace(primary.RoutePath); uri != "" {
		component.URI = uri
		if strings.TrimSpace(component.Name) == "" {
			component.Name = uri
		}
	}
	if method := strings.TrimSpace(primary.Method); method != "" {
		component.Method = method
	}
	if strings.TrimSpace(component.Name) == "" {
		component.Name = strings.TrimSpace(primary.Name)
	}
	if strings.TrimSpace(component.RootView) == "" && strings.TrimSpace(primary.ViewName) != "" {
		component.RootView = routeViewAlias(primary)
		if component.RootView != "" {
			component.Views = append(component.Views, component.RootView)
		}
	}
}

func cloneComponentRoutes(routes []*plan.ComponentRoute) []*plan.ComponentRoute {
	if len(routes) == 0 {
		return nil
	}
	result := make([]*plan.ComponentRoute, 0, len(routes))
	for _, item := range routes {
		if item == nil {
			continue
		}
		cloned := *item
		result = append(result, &cloned)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func firstComponentRoute(routes []*plan.ComponentRoute) *plan.ComponentRoute {
	for _, item := range routes {
		if item != nil {
			return item
		}
	}
	return nil
}

func materializeComponentRouteView(source *shape.Source, pResult *plan.Result, resource *view.Resource) error {
	route := firstComponentRoute(pResult.Components)
	if route == nil || resource == nil {
		return nil
	}
	viewType := resolveRouteViewType(source, route)
	if viewType == nil {
		return nil
	}
	viewName := routeViewAlias(route)
	if viewName == "" {
		viewName = "View"
	}
	opts := []view.Option{
		view.WithSchema(state.NewSchema(viewType)),
		view.WithMode(componentRouteMode(route)),
	}
	if connectorRef := strings.TrimSpace(route.Connector); connectorRef != "" {
		opts = append(opts, view.WithConnectorRef(connectorRef))
	}
	rootView := view.NewView(viewName, "", opts...)
	if sourceURL := absoluteRouteSourceURL(source, route); sourceURL != "" {
		tmpl := view.NewTemplate("")
		tmpl.SourceURL = sourceURL
		rootView.Template = tmpl
	}
	resource.AddViews(rootView)
	return nil
}

func componentRouteMode(route *plan.ComponentRoute) view.Mode {
	if route == nil {
		return view.ModeQuery
	}
	if strings.TrimSpace(route.Handler) != "" {
		return view.ModeHandler
	}
	switch strings.ToUpper(strings.TrimSpace(route.Method)) {
	case "", "GET":
		return view.ModeQuery
	default:
		return view.ModeExec
	}
}

func resolveRouteViewType(source *shape.Source, route *plan.ComponentRoute) reflect.Type {
	if source == nil || route == nil {
		return nil
	}
	typeName := strings.TrimSpace(route.ViewName)
	if typeName == "" {
		return nil
	}
	registry := source.EnsureTypeRegistry()
	if registry == nil {
		return nil
	}
	if lookup := registry.Lookup(typeName); lookup != nil && lookup.Type != nil {
		return lookup.Type
	}
	resolver := typectx.NewResolver(registry, nil)
	if resolved, err := resolver.Resolve(typeName); err == nil && resolved != "" {
		if lookup := registry.Lookup(resolved); lookup != nil && lookup.Type != nil {
			return lookup.Type
		}
	}
	return nil
}

func routeViewAlias(route *plan.ComponentRoute) string {
	if route == nil {
		return ""
	}
	if name := strings.TrimSpace(route.Name); name != "" {
		return name
	}
	viewName := strings.TrimSpace(route.ViewName)
	if index := strings.LastIndex(viewName, "."); index >= 0 {
		viewName = viewName[index+1:]
	}
	viewName = strings.TrimSuffix(viewName, "View")
	return strings.TrimSpace(viewName)
}

func absoluteRouteSourceURL(source *shape.Source, route *plan.ComponentRoute) string {
	if route == nil {
		return ""
	}
	sourceURL := strings.TrimSpace(route.SourceURL)
	return absolutizeRouteAssetURL(source, sourceURL)
}

func absoluteRouteSummaryURL(source *shape.Source, route *plan.ComponentRoute) string {
	if route == nil {
		return ""
	}
	return absolutizeRouteAssetURL(source, strings.TrimSpace(route.SummaryURL))
}

func absolutizeRouteAssetURL(source *shape.Source, sourceURL string) string {
	if sourceURL == "" || strings.Contains(sourceURL, "://") {
		return sourceURL
	}
	if filepath.IsAbs(sourceURL) {
		return sourceURL
	}
	baseDir := ""
	if source != nil {
		baseDir = source.BaseDir()
	}
	if baseDir == "" {
		return sourceURL
	}
	return filepath.Join(baseDir, filepath.FromSlash(sourceURL))
}

func resolveTypeSpecs(pResult *plan.Result) map[string]*TypeSpec {
	if pResult == nil {
		return nil
	}
	specs := map[string]*TypeSpec{}
	directives := pResult.Directives
	if directives != nil {
		if typeName := strings.TrimSpace(directives.InputType); typeName != "" {
			specs["input"] = &TypeSpec{Key: "input", Role: TypeRoleInput, TypeName: typeName, Source: "directive"}
		}
		if typeName := strings.TrimSpace(directives.OutputType); typeName != "" {
			specs["output"] = &TypeSpec{Key: "output", Role: TypeRoleOutput, TypeName: typeName, Source: "directive"}
		}
		if dest := strings.TrimSpace(directives.InputDest); dest != "" {
			spec := ensureTypeSpec(specs, "input", TypeRoleInput)
			spec.Dest = dest
			spec.Source = "directive"
		}
		if dest := strings.TrimSpace(directives.OutputDest); dest != "" {
			spec := ensureTypeSpec(specs, "output", TypeRoleOutput)
			spec.Dest = dest
			spec.Source = "directive"
		}
	}
	globalDest := ""
	if directives != nil {
		globalDest = strings.TrimSpace(directives.Dest)
	}
	for _, aView := range pResult.Views {
		if aView == nil || strings.TrimSpace(aView.Name) == "" {
			continue
		}
		key := "view:" + aView.Name
		spec := ensureTypeSpec(specs, key, TypeRoleView)
		spec.Alias = aView.Name
		if globalDest != "" && spec.Dest == "" {
			spec.Dest = globalDest
			spec.Inherited = true
			spec.Source = "directive"
		}
		if aView.Declaration != nil {
			if typeName := strings.TrimSpace(aView.Declaration.TypeName); typeName != "" {
				spec.TypeName = typeName
				spec.Source = "decl"
			}
			if dest := strings.TrimSpace(aView.Declaration.Dest); dest != "" {
				spec.Dest = dest
				spec.Inherited = false
				spec.Source = "decl"
			}
			if tagType, tagDest := parseTypeSpecTag(aView.Declaration.Tag); tagType != "" || tagDest != "" {
				if spec.TypeName == "" && tagType != "" {
					spec.TypeName = tagType
					spec.Source = "annotation"
				}
				if spec.Dest == "" && tagDest != "" {
					spec.Dest = tagDest
					spec.Inherited = false
					spec.Source = "annotation"
				}
			}
		}
	}
	if root := pickRootView(pResult.Views); root != nil {
		if rootSpec := specs["view:"+root.Name]; rootSpec != nil && strings.TrimSpace(rootSpec.Dest) != "" {
			rootDest := strings.TrimSpace(rootSpec.Dest)
			for _, aView := range pResult.Views {
				if aView == nil || strings.TrimSpace(aView.Name) == "" || aView.Name == root.Name {
					continue
				}
				spec := ensureTypeSpec(specs, "view:"+aView.Name, TypeRoleView)
				spec.Alias = aView.Name
				if strings.TrimSpace(spec.Dest) == "" || spec.Source == "directive" || spec.Source == "inherit" {
					spec.Dest = rootDest
					spec.Inherited = true
					spec.Source = "inherit"
				}
			}
		}
	}
	if globalDest != "" {
		inputSpec := ensureTypeSpec(specs, "input", TypeRoleInput)
		if strings.TrimSpace(inputSpec.Dest) == "" {
			inputSpec.Dest = globalDest
			inputSpec.Inherited = true
			if inputSpec.Source == "" {
				inputSpec.Source = "directive"
			}
		}
		outputSpec := ensureTypeSpec(specs, "output", TypeRoleOutput)
		if strings.TrimSpace(outputSpec.Dest) == "" {
			outputSpec.Dest = globalDest
			outputSpec.Inherited = true
			if outputSpec.Source == "" {
				outputSpec.Source = "directive"
			}
		}
	}
	if len(specs) == 0 {
		return nil
	}
	return specs
}

func ensureTypeSpec(specs map[string]*TypeSpec, key string, role TypeRole) *TypeSpec {
	if spec, ok := specs[key]; ok && spec != nil {
		return spec
	}
	spec := &TypeSpec{Key: key, Role: role}
	specs[key] = spec
	return spec
}

func parseTypeSpecTag(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	var typeName, dest string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(strings.Trim(value, `"'`))
		switch key {
		case "type":
			typeName = value
		case "dest":
			dest = value
		}
	}
	return strings.TrimSpace(typeName), strings.TrimSpace(dest)
}

// applyViewMeta populates the component with view names, declarations, relations,
// query selectors, predicate maps, and root view from the plan view list.
func applyViewMeta(component *Component, views []*plan.View) {
	for _, aView := range views {
		if aView == nil {
			continue
		}
		component.Views = append(component.Views, aView.Name)
		if aView.Declaration != nil {
			indexViewDeclaration(component, declaredViewIndexName(aView), aView.Declaration)
		}
		if len(aView.Relations) > 0 {
			component.Relations = append(component.Relations, aView.Relations...)
			component.ViewRelations = append(component.ViewRelations, toViewRelations(aView.Relations)...)
		}
	}
	if rootView := pickRootView(views); rootView != nil {
		component.RootView = rootView.Name
		if component.Name == "" {
			component.Name = rootView.Name
		}
	}
}

func declaredViewIndexName(aView *plan.View) string {
	if aView == nil {
		return ""
	}
	if queryNode, err := sqlparser.ParseQuery(strings.TrimSpace(aView.SQL)); err == nil && queryNode != nil {
		if inferredName, _, err := pipeline.InferRoot(queryNode, aView.Name); err == nil && strings.TrimSpace(inferredName) != "" {
			return inferredName
		}
	}
	return aView.Name
}

// indexViewDeclaration registers the declaration's query selector and predicates
// on the component index maps, creating them on demand.
func indexViewDeclaration(component *Component, viewName string, decl *plan.ViewDeclaration) {
	if component.Declarations == nil {
		component.Declarations = map[string]*plan.ViewDeclaration{}
	}
	component.Declarations[viewName] = decl
	if selector := strings.TrimSpace(decl.QuerySelector); selector != "" {
		if component.QuerySelectors == nil {
			component.QuerySelectors = map[string][]string{}
		}
		component.QuerySelectors[selector] = append(component.QuerySelectors[selector], viewName)
	}
	if len(decl.Predicates) > 0 {
		if component.Predicates == nil {
			component.Predicates = map[string][]*plan.ViewPredicate{}
		}
		component.Predicates[viewName] = append(component.Predicates[viewName], decl.Predicates...)
	}
}

// applyStateBuckets sorts plan states into the typed buckets on the component
// (Input, Output, Meta, Async, Other) based on the state's location kind.
func applyStateBuckets(component *Component, states []*plan.State, resource *view.Resource, loadOptions *shape.LoadOptions) {
	for _, item := range states {
		if item == nil {
			continue
		}
		cloned := clonePlanState(item)
		if cloned == nil {
			continue
		}
		if loadOptions != nil && loadOptions.UseTypeContextPackages {
			inheritTypeContextSchemaPackage(&cloned.Parameter, component)
		}
		normalizeDerivedInputSchema(&cloned.Parameter, resource)
		inheritRootBodySchema(&cloned.Parameter, rootResourceView(resource, nil))
		kind := state.Kind(strings.ToLower(item.KindString()))
		inName := item.InName()
		if kind == "" && inName == "" {
			component.Other = append(component.Other, cloned)
			continue
		}
		switch kind {
		case state.KindQuery, state.KindPath, state.KindHeader, state.KindRequestBody,
			state.KindView, state.KindComponent, state.KindConst,
			state.KindForm, state.KindCookie, state.KindRequest, "":
			component.Input = append(component.Input, cloned)
		case state.KindOutput:
			component.Output = append(component.Output, cloned)
		case state.KindMeta:
			component.Meta = append(component.Meta, cloned)
		case state.KindAsync:
			component.Async = append(component.Async, cloned)
		default:
			component.Other = append(component.Other, cloned)
		}
	}
}

func inheritTypeContextSchemaPackage(param *state.Parameter, component *Component) {
	if param == nil || param.Schema == nil || component == nil || component.TypeContext == nil {
		return
	}
	if param.Schema.Type() != nil {
		return
	}
	if strings.TrimSpace(param.Schema.Package) != "" || strings.TrimSpace(param.Schema.PackagePath) != "" {
		return
	}
	typeName := strings.TrimSpace(shared.FirstNotEmpty(param.Schema.Name, param.Schema.DataType))
	if typeName == "" || strings.Contains(typeName, ".") {
		return
	}
	if _, err := types.LookupType(nil, typeName); err == nil {
		return
	}
	pkgPath := strings.TrimSpace(component.TypeContext.PackagePath)
	if pkgPath == "" {
		pkgPath = strings.TrimSpace(component.TypeContext.DefaultPackage)
	}
	if pkgPath == "" {
		return
	}
	param.Schema.Package = pkgPath
	param.Schema.PackagePath = pkgPath
}

func inheritViewSchemaPackage(aView *view.View, ctx *typectx.Context) {
	if aView == nil || aView.Schema == nil || ctx == nil {
		return
	}
	if aView.Schema.Type() != nil {
		return
	}
	if strings.TrimSpace(aView.Schema.Package) != "" || strings.TrimSpace(aView.Schema.PackagePath) != "" {
		return
	}
	typeName := strings.TrimSpace(shared.FirstNotEmpty(aView.Schema.Name, aView.Schema.DataType))
	if typeName == "" || strings.Contains(typeName, ".") {
		return
	}
	if _, err := types.LookupType(nil, typeName); err == nil {
		return
	}
	pkgPath := strings.TrimSpace(ctx.PackagePath)
	if pkgPath == "" {
		pkgPath = strings.TrimSpace(ctx.DefaultPackage)
	}
	if pkgPath == "" {
		return
	}
	aView.Schema.Package = pkgPath
	aView.Schema.PackagePath = pkgPath
}

func synthesizeMissingRouteContractStates(component *Component, routes []*plan.ComponentRoute) []*plan.State {
	if component == nil || len(routes) == 0 {
		return nil
	}
	declared := map[string]bool{}
	register := func(items []*plan.State) {
		for _, item := range items {
			if item == nil || item.In == nil {
				continue
			}
			declared[routeContractStateKey(item.Name, item.In.Kind, item.In.Name)] = true
		}
	}
	register(component.Input)
	register(component.Output)
	register(component.Meta)
	register(component.Async)
	register(component.Other)

	var result []*plan.State
	for _, route := range routes {
		if route == nil {
			continue
		}
		for _, item := range contractStates(route.InputType) {
			key := routeContractStateKey(item.Name, item.In.Kind, item.In.Name)
			if declared[key] {
				continue
			}
			declared[key] = true
			result = append(result, item)
		}
		for _, item := range contractStates(route.OutputType) {
			key := routeContractStateKey(item.Name, item.In.Kind, item.In.Name)
			if declared[key] {
				continue
			}
			declared[key] = true
			result = append(result, item)
		}
	}
	return result
}

func contractStates(rType reflect.Type) []*plan.State {
	rType = unwrapContractStateType(rType)
	if rType == nil || rType.Kind() != reflect.Struct {
		return nil
	}
	var result []*plan.State
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if field.Anonymous {
			result = append(result, contractStates(field.Type)...)
		}
		parsed, err := tags.ParseStateTags(field.Tag, nil)
		if err != nil || parsed == nil || parsed.Parameter == nil {
			continue
		}
		param := parsed.Parameter
		name := strings.TrimSpace(param.Name)
		if name == "" {
			name = strings.TrimSpace(field.Name)
		}
		locationKind := state.Kind(strings.ToLower(strings.TrimSpace(param.Kind)))
		locationName := strings.TrimSpace(param.In)
		item := &plan.State{
			Parameter: state.Parameter{
				Name:            name,
				In:              &state.Location{Kind: locationKind, Name: locationName},
				When:            param.When,
				Scope:           param.Scope,
				Required:        param.Required,
				Async:           param.Async,
				Cacheable:       param.Cacheable,
				With:            param.With,
				URI:             param.URI,
				ErrorStatusCode: param.ErrorCode,
				ErrorMessage:    param.ErrorMessage,
				Tag:             string(field.Tag),
				Schema:          state.NewSchema(field.Type),
			},
		}
		state.BuildCodec(parsed, &item.Parameter)
		state.BuildHandler(parsed, &item.Parameter)
		if dataType := strings.TrimSpace(param.DataType); dataType != "" && item.Schema != nil {
			item.Schema.DataType = dataType
		}
		result = append(result, item)
	}
	return result
}

func unwrapContractStateType(rType reflect.Type) reflect.Type {
	for rType != nil && (rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array) {
		rType = rType.Elem()
	}
	return rType
}

func routeContractStateKey(name string, kind state.Kind, in string) string {
	return strings.ToLower(strings.TrimSpace(name)) + "|" + strings.ToLower(strings.TrimSpace(string(kind))) + "|" + strings.ToLower(strings.TrimSpace(in))
}

// synthesizePredicateStates creates query parameters for view-level predicates whose
// source parameter is not already present in the input state list.
func synthesizePredicateStates(input []*plan.State, predicates map[string][]*plan.ViewPredicate) []*plan.State {
	if len(predicates) == 0 {
		return nil
	}
	declared := make(map[string]bool, len(input))
	for _, s := range input {
		if s != nil {
			declared[strings.ToLower(strings.TrimPrefix(strings.TrimSpace(s.Name), "$"))] = true
		}
	}
	var result []*plan.State
	for _, viewPredicates := range predicates {
		for _, vp := range viewPredicates {
			if vp == nil {
				continue
			}
			src := strings.TrimPrefix(strings.TrimSpace(vp.Source), "$")
			if src == "" || declared[strings.ToLower(src)] {
				continue
			}
			result = append(result, &plan.State{
				Parameter: state.Parameter{
					Name:   src,
					In:     state.NewQueryLocation(src),
					Schema: &state.Schema{DataType: "string"},
					Predicates: []*extension.PredicateConfig{
						{
							Name:   vp.Name,
							Ensure: vp.Ensure,
							Args:   append([]string{}, vp.Arguments...),
						},
					},
				},
			})
			declared[strings.ToLower(src)] = true
		}
	}
	return result
}

func synthesizeConstStates(constants map[string]string) []*plan.State {
	if len(constants) == 0 {
		return nil
	}
	keys := make([]string, 0, len(constants))
	for key := range constants {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]*plan.State, 0, len(keys))
	for _, key := range keys {
		result = append(result, &plan.State{
			Parameter: state.Parameter{
				Name:  key,
				In:    state.NewConstLocation(key),
				Value: constants[key],
				Tag:   `internal:"true"`,
				Schema: &state.Schema{
					Name:        "string",
					DataType:    "string",
					Cardinality: state.One,
				},
			},
		})
	}
	return result
}

func cloneTypeContext(input *typectx.Context) *typectx.Context {
	if input == nil {
		return nil
	}
	ret := &typectx.Context{
		DefaultPackage: strings.TrimSpace(input.DefaultPackage),
		PackageDir:     strings.TrimSpace(input.PackageDir),
		PackageName:    strings.TrimSpace(input.PackageName),
		PackagePath:    strings.TrimSpace(input.PackagePath),
	}
	for _, item := range input.Imports {
		pkg := strings.TrimSpace(item.Package)
		if pkg == "" {
			continue
		}
		ret.Imports = append(ret.Imports, typectx.Import{
			Alias:   strings.TrimSpace(item.Alias),
			Package: pkg,
		})
	}
	if ret.DefaultPackage == "" &&
		len(ret.Imports) == 0 &&
		ret.PackageDir == "" &&
		ret.PackageName == "" &&
		ret.PackagePath == "" {
		return nil
	}
	return ret
}

func cloneDirectives(input *dqlshape.Directives) *dqlshape.Directives {
	if input == nil {
		return nil
	}
	ret := &dqlshape.Directives{
		Meta:             strings.TrimSpace(input.Meta),
		DefaultConnector: strings.TrimSpace(input.DefaultConnector),
		Dest:             strings.TrimSpace(input.Dest),
		InputDest:        strings.TrimSpace(input.InputDest),
		OutputDest:       strings.TrimSpace(input.OutputDest),
		RouterDest:       strings.TrimSpace(input.RouterDest),
		InputType:        strings.TrimSpace(input.InputType),
		OutputType:       strings.TrimSpace(input.OutputType),
	}
	if input.Cache != nil {
		ret.Cache = &dqlshape.CacheDirective{
			Enabled:      input.Cache.Enabled,
			TTL:          strings.TrimSpace(input.Cache.TTL),
			Name:         strings.TrimSpace(input.Cache.Name),
			Provider:     strings.TrimSpace(input.Cache.Provider),
			Location:     strings.TrimSpace(input.Cache.Location),
			TimeToLiveMs: input.Cache.TimeToLiveMs,
		}
	}
	if input.MCP != nil {
		ret.MCP = &dqlshape.MCPDirective{
			Name:            strings.TrimSpace(input.MCP.Name),
			Description:     strings.TrimSpace(input.MCP.Description),
			DescriptionPath: strings.TrimSpace(input.MCP.DescriptionPath),
		}
	}
	if input.Const != nil {
		ret.Const = make(map[string]string, len(input.Const))
		for k, v := range input.Const {
			ret.Const[k] = v
		}
	}
	if input.Route != nil {
		ret.Route = &dqlshape.RouteDirective{
			URI: strings.TrimSpace(input.Route.URI),
		}
		for _, m := range input.Route.Methods {
			if m = strings.TrimSpace(m); m != "" {
				ret.Route.Methods = append(ret.Route.Methods, m)
			}
		}
	}
	if ret.Meta == "" && ret.DefaultConnector == "" &&
		ret.Dest == "" && ret.InputDest == "" && ret.OutputDest == "" && ret.RouterDest == "" &&
		ret.InputType == "" && ret.OutputType == "" &&
		ret.Cache == nil && ret.MCP == nil && ret.Route == nil && len(ret.Const) == 0 {
		return nil
	}
	return ret
}

func pickRootView(views []*plan.View) *plan.View {
	var selected *plan.View
	minDepth := -1
	for _, candidate := range views {
		if candidate == nil || candidate.Path == "" {
			continue
		}
		depth := strings.Count(candidate.Path, ".")
		if minDepth == -1 || depth < minDepth {
			minDepth = depth
			selected = candidate
		}
	}
	if selected != nil {
		return selected
	}
	for _, candidate := range views {
		if candidate != nil {
			return candidate
		}
	}
	return nil
}

func materializeView(item *plan.View) (*view.View, error) {
	if item == nil {
		return nil, fmt.Errorf("shape load: nil view plan item")
	}

	schemaType := bestSchemaType(item)
	mode := view.ModeQuery
	switch strings.TrimSpace(item.Mode) {
	case string(view.ModeExec):
		mode = view.ModeExec
	case string(view.ModeHandler):
		mode = view.ModeHandler
	case string(view.ModeQuery):
		mode = view.ModeQuery
	}
	if shouldDeferQuerySchemaType(schemaType, mode) {
		schemaType = nil
	}
	if schemaType == nil && !allowsDeferredSchema(item, mode) {
		return nil, fmt.Errorf("shape load: missing schema type for view %q", item.Name)
	}

	schema := newSchema(schemaType, item.Cardinality)
	opts := []view.Option{view.WithSchema(schema), view.WithMode(mode)}

	if item.Connector != "" {
		opts = append(opts, view.WithConnectorRef(item.Connector))
	}
	if item.SQL != "" || item.SQLURI != "" {
		tmpl := view.NewTemplate(item.SQL)
		tmpl.SourceURL = item.SQLURI
		if strings.TrimSpace(item.Summary) != "" {
			name := strings.TrimSpace(item.SummaryName)
			if name == "" {
				name = "Summary"
			}
			tmpl.Summary = &view.TemplateSummary{
				Name:   name,
				Source: item.Summary,
				Kind:   view.MetaKindRecord,
			}
		}
		opts = append(opts, view.WithTemplate(tmpl))
	}
	if item.CacheRef != "" {
		opts = append(opts, view.WithCache(&view.Cache{Reference: shared.Reference{Ref: item.CacheRef}}))
	}
	if item.Partitioner != "" {
		opts = append(opts, view.WithPartitioned(&view.Partitioned{
			DataType:    item.Partitioner,
			Concurrency: item.PartitionedConcurrency,
		}))
	}

	aView, err := view.New(item.Name, item.Table, opts...)
	if err != nil {
		return nil, err
	}
	aView.Ref = item.Ref
	aView.Module = item.Module
	aView.AllowNulls = item.AllowNulls
	// Gap 6: forward view-level tag from declaration.
	if item.Declaration != nil && strings.TrimSpace(item.Declaration.Tag) != "" {
		aView.Tag = strings.TrimSpace(item.Declaration.Tag)
	}
	if item.Declaration != nil && len(item.Declaration.ColumnsConfig) > 0 {
		if aView.ColumnsConfig == nil {
			aView.ColumnsConfig = map[string]*view.ColumnConfig{}
		}
		for name, cfg := range item.Declaration.ColumnsConfig {
			name = strings.TrimSpace(name)
			if name == "" || cfg == nil {
				continue
			}
			columnCfg := aView.ColumnsConfig[name]
			if columnCfg == nil {
				columnCfg = &view.ColumnConfig{Name: name}
				aView.ColumnsConfig[name] = columnCfg
			}
			if dataType := strings.TrimSpace(cfg.DataType); dataType != "" {
				columnCfg.DataType = stringPtr(dataType)
			}
			if tag := strings.TrimSpace(cfg.Tag); tag != "" {
				columnCfg.Tag = stringPtr(tag)
			}
		}
	}
	if strings.TrimSpace(item.SelectorNamespace) != "" || item.SelectorNoLimit != nil || item.SelectorLimit != nil {
		if aView.Selector == nil {
			aView.Selector = &view.Config{}
		}
		if strings.TrimSpace(item.SelectorNamespace) != "" {
			aView.Selector.Namespace = strings.TrimSpace(item.SelectorNamespace)
		}
		if item.SelectorNoLimit != nil {
			aView.Selector.NoLimit = *item.SelectorNoLimit
		}
		if item.SelectorLimit != nil {
			aView.Selector.Limit = *item.SelectorLimit
		}
	}
	if item.Self != nil {
		aView.SelfReference = &view.SelfReference{
			Holder: item.Self.Holder,
			Child:  item.Self.Child,
			Parent: item.Self.Parent,
		}
	}
	if aView.Schema != nil && strings.TrimSpace(item.SchemaType) != "" {
		if aView.Schema.DataType == "" {
			aView.Schema.DataType = strings.TrimSpace(item.SchemaType)
		}
		if aView.Schema.Name == "" {
			aView.Schema.Name = strings.Trim(strings.TrimSpace(item.SchemaType), "*")
		}
	}
	// Populate columns from statically-inferred struct type so that xgen can
	// generate accurate Go struct definitions during bootstrap. Only applied when
	// the view has no columns yet (avoids overwriting explicit column config).
	if len(aView.Columns) == 0 {
		if cols := inferColumnsFromType(bestSchemaType(item)); len(cols) > 0 && !inferredColumnsArePlaceholders(cols) {
			aView.Columns = cols
		}
	}
	return aView, nil
}

func allowsDeferredSchema(item *plan.View, mode view.Mode) bool {
	if item == nil {
		return false
	}
	if mode != view.ModeQuery {
		return false
	}
	return strings.TrimSpace(item.Table) != "" || strings.TrimSpace(item.SQL) != "" || strings.TrimSpace(item.SQLURI) != ""
}

func shouldDeferQuerySchemaType(rType reflect.Type, mode view.Mode) bool {
	if rType == nil || mode != view.ModeQuery {
		return false
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
	}
	return rType.Kind() == reflect.Map || rType.Kind() == reflect.Interface
}

func bestSchemaType(item *plan.View) reflect.Type {
	if item.FieldType != nil {
		return item.FieldType
	}
	if item.ElementType != nil {
		return item.ElementType
	}
	return nil
}

func stringPtr(value string) *string {
	ret := value
	return &ret
}

func toViewRelations(input []*plan.Relation) []*view.Relation {
	if len(input) == 0 {
		return nil
	}
	result := make([]*view.Relation, 0, len(input))
	for _, item := range input {
		if item == nil {
			continue
		}
		relation := &view.Relation{
			Name:          item.Name,
			Holder:        item.Holder,
			Cardinality:   state.Many,
			IncludeColumn: true,
			On:            toViewLinks(item.On, true),
			Of: view.NewReferenceView(
				toViewLinks(item.On, false),
				view.NewView(item.Ref, item.Table),
			),
		}
		result = append(result, relation)
	}
	return result
}

func toViewLinks(input []*plan.RelationLink, parent bool) view.Links {
	if len(input) == 0 {
		return nil
	}
	result := make(view.Links, 0, len(input))
	for _, item := range input {
		if item == nil {
			continue
		}
		link := &view.Link{}
		if parent {
			link.Field = item.ParentField
			link.Namespace = item.ParentNamespace
			link.Column = item.ParentColumn
		} else {
			link.Field = item.RefField
			link.Namespace = item.RefNamespace
			link.Column = item.RefColumn
		}
		result = append(result, link)
	}
	return result
}

func newSchema(rType reflect.Type, cardinality string) *state.Schema {
	if rType == nil {
		schema := &state.Schema{}
		if cardinality == "many" {
			schema.Cardinality = state.Many
		} else {
			schema.Cardinality = state.One
		}
		return schema
	}
	if cardinality == "many" && rType.Kind() != reflect.Slice {
		return state.NewSchema(rType, state.WithMany())
	}
	return state.NewSchema(rType)
}

func attachViewRelations(resource *view.Resource, planned []*plan.View) {
	if resource == nil || len(planned) == 0 {
		return
	}
	index := resource.Views.Index()
	byName := map[string]*plan.View{}
	for _, item := range planned {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		byName[strings.ToLower(strings.TrimSpace(item.Name))] = item
	}
	for _, item := range planned {
		if item == nil || len(item.Relations) == 0 {
			continue
		}
		candidates := toViewRelations(item.Relations)
		for i, relation := range candidates {
			if relation == nil || relation.Of == nil {
				continue
			}
			parentName := relationParentName(item, item.Relations, i)
			if parentName == "" {
				continue
			}
			parent, err := index.Lookup(parentName)
			if err != nil || parent == nil {
				continue
			}
			if plannedParent, ok := byName[strings.ToLower(parentName)]; ok && plannedParent != nil {
				parentName = plannedParent.Name
			}
			refName := strings.TrimSpace(relation.Of.View.Ref)
			if refName == "" {
				refName = strings.TrimSpace(relation.Of.View.Name)
			}
			if refName == "" {
				continue
			}
			ref, err := index.Lookup(refName)
			if err != nil || ref == nil {
				continue
			}
			if plannedRef, ok := byName[strings.ToLower(refName)]; ok && plannedRef != nil {
				if strings.EqualFold(strings.TrimSpace(plannedRef.Cardinality), string(state.One)) {
					relation.Cardinality = state.One
				}
			}
			if inferOneToOneRelation(parent, ref, relation) {
				relation.Cardinality = state.One
			}
			relation.Of.View.Ref = ref.Name
			relation.Of.View.Name = ""
			relation.Of.View.Columns = ref.Columns
			parent.With = append(parent.With, relation)
		}
	}
}

func enrichRelationHolderTypes(resource *view.Resource, planned []*plan.View) error {
	if resource == nil || len(planned) == 0 {
		return nil
	}
	index := resource.Views.Index()
	byName := map[string]*plan.View{}
	for _, item := range planned {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		byName[strings.ToLower(strings.TrimSpace(item.Name))] = item
	}
	for _, item := range planned {
		if item == nil || len(item.Relations) == 0 {
			continue
		}
		for i, rel := range item.Relations {
			if rel == nil {
				continue
			}
			parentName := relationParentName(item, item.Relations, i)
			if parentName == "" {
				parentName = item.Name
			}
			parent, err := index.Lookup(parentName)
			if err != nil || parent == nil || parent.Schema == nil {
				continue
			}
			parentType := parent.ComponentType()
			if parentType == nil {
				continue
			}
			augmented, changed, err := ensureRelationHolderFields(parentType, &plan.View{Relations: []*plan.Relation{rel}}, byName, index)
			if err != nil {
				return err
			}
			if !changed || augmented == nil {
				continue
			}
			if parent.Schema.Cardinality == state.Many {
				parent.Schema.SetType(reflect.SliceOf(augmented))
				continue
			}
			parent.Schema.SetType(augmented)
		}
	}
	return nil
}

func ensureRelationHolderFields(parentType reflect.Type, item *plan.View, byName map[string]*plan.View, index view.NamedViews) (reflect.Type, bool, error) {
	parentType = ensureStructType(parentType)
	if parentType == nil || item == nil || len(item.Relations) == 0 {
		return parentType, false, nil
	}
	fields := make([]reflect.StructField, 0, parentType.NumField()+len(item.Relations))
	for i := 0; i < parentType.NumField(); i++ {
		fields = append(fields, parentType.Field(i))
	}
	changed := false
	for _, rel := range item.Relations {
		if rel == nil || strings.TrimSpace(rel.Holder) == "" {
			continue
		}
		if _, ok := parentType.FieldByName(rel.Holder); ok {
			continue
		}
		childName := strings.TrimSpace(rel.Ref)
		if childName == "" {
			continue
		}
		childView, err := index.Lookup(childName)
		if err != nil || childView == nil {
			continue
		}
		childType := childView.ComponentType()
		if childType == nil && childView.Schema != nil {
			childType = childView.Schema.Type()
		}
		if childType == nil {
			if childPlanned, ok := byName[strings.ToLower(childName)]; ok && childPlanned != nil {
				childType = bestSchemaType(childPlanned)
			}
		}
		if childType == nil {
			continue
		}
		fieldType := relationHolderFieldType(childType, childPlannedCardinality(childName, byName))
		if fieldType == nil {
			continue
		}
		fields = append(fields, reflect.StructField{
			Name: rel.Holder,
			Type: fieldType,
			Tag:  reflect.StructTag(buildRelationHolderTag(rel, childView)),
		})
		changed = true
	}
	if !changed {
		return parentType, false, nil
	}
	return reflect.StructOf(fields), true, nil
}

func childPlannedCardinality(childName string, byName map[string]*plan.View) state.Cardinality {
	if childPlanned, ok := byName[strings.ToLower(childName)]; ok && childPlanned != nil {
		if strings.EqualFold(strings.TrimSpace(childPlanned.Cardinality), string(state.One)) {
			return state.One
		}
	}
	return state.Many
}

func relationHolderFieldType(childType reflect.Type, cardinality state.Cardinality) reflect.Type {
	if childType == nil {
		return nil
	}
	childType = normalizeDeferredHolderType(childType)
	if cardinality == state.One {
		for childType.Kind() == reflect.Slice || childType.Kind() == reflect.Array {
			childType = childType.Elem()
		}
		if childType.Kind() == reflect.Struct {
			return reflect.PtrTo(childType)
		}
		return childType
	}
	if childType.Kind() == reflect.Slice || childType.Kind() == reflect.Array {
		return childType
	}
	normalized := childType
	if normalized.Kind() == reflect.Struct {
		normalized = reflect.PtrTo(normalized)
	}
	return reflect.SliceOf(normalized)
}

func normalizeDeferredHolderType(rType reflect.Type) reflect.Type {
	if rType == nil {
		return nil
	}
	kind := rType.Kind()
	if kind == reflect.Slice || kind == reflect.Array {
		elem := rType.Elem()
		for elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		if elem.Kind() == reflect.Map || elem.Kind() == reflect.Interface {
			return reflect.SliceOf(reflect.TypeOf(struct{}{}))
		}
		return rType
	}
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType.Kind() == reflect.Map || rType.Kind() == reflect.Interface {
		return reflect.TypeOf(struct{}{})
	}
	return rType
}

func ensureStructType(rType reflect.Type) reflect.Type {
	if rType == nil {
		return nil
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	return rType
}

func buildRelationHolderTag(rel *plan.Relation, child *view.View) string {
	if rel == nil {
		return `json:",omitempty" sqlx:"-"`
	}
	table := ""
	sqlExpr := ""
	if child != nil {
		table = strings.TrimSpace(child.Table)
		if child.Template != nil {
			if uri := strings.TrimSpace(child.Template.SourceURL); uri != "" {
				sqlExpr = "uri=" + uri
			} else if source := strings.TrimSpace(child.Template.Source); source != "" {
				sqlExpr = source
			}
		}
	}
	tagParts := []string{fmt.Sprintf(`view:",table=%s"`, table)}
	if onExpr := buildRelationOnTag(rel); onExpr != "" {
		tagParts = append(tagParts, fmt.Sprintf(`on:"%s"`, onExpr))
	}
	if sqlExpr != "" {
		tagParts = append(tagParts, fmt.Sprintf(`sql:%q`, sqlExpr))
	}
	tagParts = append(tagParts, `json:",omitempty"`, `sqlx:"-"`)
	return strings.Join(tagParts, " ")
}

func buildRelationOnTag(rel *plan.Relation) string {
	if rel == nil || len(rel.On) == 0 {
		return ""
	}
	parts := make([]string, 0, len(rel.On))
	for _, link := range rel.On {
		if link == nil {
			continue
		}
		parentField := firstNonEmpty(strings.TrimSpace(link.ParentField), strings.TrimSpace(link.ParentColumn))
		refField := firstNonEmpty(strings.TrimSpace(link.RefField), strings.TrimSpace(link.RefColumn))
		if parentField == "" || refField == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s:%s=%s:%s", parentField, link.ParentColumn, refField, link.RefColumn))
	}
	return strings.Join(parts, ",")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func bindTemplateParameters(resource *view.Resource) {
	if resource == nil || len(resource.Parameters) == 0 {
		return
	}
	params := make([]*state.Parameter, 0, len(resource.Parameters))
	for _, param := range resource.Parameters {
		if param == nil || param.In == nil {
			continue
		}
		switch param.In.Kind {
		case state.KindOutput, state.KindMeta, state.KindAsync:
			continue
		}
		params = append(params, param)
	}
	if len(params) == 0 {
		return
	}
	for _, item := range resource.Views {
		bindViewTemplateParameters(item, params)
	}
}

func bindViewTemplateParameters(aView *view.View, params []*state.Parameter) {
	if aView == nil {
		return
	}
	if aView.Template != nil {
		seen := map[string]bool{}
		for _, item := range aView.Template.Parameters {
			if item != nil {
				seen[strings.ToLower(strings.TrimSpace(item.Name))] = true
			}
		}
		for _, param := range params {
			if param == nil || strings.TrimSpace(param.Name) == "" {
				continue
			}
			if param.In != nil && param.In.Kind == state.KindView && strings.EqualFold(strings.TrimSpace(param.In.Name), strings.TrimSpace(aView.Name)) {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(param.Name))
			if seen[key] {
				continue
			}
			aView.Template.Parameters = append(aView.Template.Parameters, param)
			seen[key] = true
		}
	}
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		bindViewTemplateParameters(&rel.Of.View, params)
	}
}

func inferOneToOneRelation(parent, ref *view.View, relation *view.Relation) bool {
	if parent == nil || ref == nil || relation == nil || relation.Of == nil {
		return false
	}
	parentTable := strings.TrimSpace(parent.Table)
	refTable := strings.TrimSpace(ref.Table)
	if parentTable == "" || refTable == "" || !strings.EqualFold(parentTable, refTable) {
		return false
	}
	if len(relation.On) == 0 || len(relation.Of.On) == 0 {
		return false
	}
	count := len(relation.On)
	if len(relation.Of.On) < count {
		count = len(relation.Of.On)
	}
	if count == 0 {
		return false
	}
	for i := 0; i < count; i++ {
		parentCol := normalizeRelationColumn(relation.On[i].Column)
		refCol := normalizeRelationColumn(relation.Of.On[i].Column)
		if parentCol == "" || refCol == "" || !strings.EqualFold(parentCol, refCol) {
			return false
		}
	}
	return true
}

func normalizeRelationColumn(column string) string {
	column = strings.TrimSpace(column)
	if column == "" {
		return ""
	}
	if idx := strings.LastIndex(column, "."); idx != -1 && idx+1 < len(column) {
		column = column[idx+1:]
	}
	return strings.TrimSpace(column)
}

func relationParentName(source *plan.View, relations []*plan.Relation, index int) string {
	if index >= 0 && index < len(relations) {
		item := relations[index]
		if item != nil {
			if parent := strings.TrimSpace(item.Parent); parent != "" {
				return parent
			}
			for _, link := range item.On {
				if link == nil {
					continue
				}
				if parent := strings.TrimSpace(link.ParentNamespace); parent != "" {
					return parent
				}
			}
		}
	}
	if source == nil {
		return ""
	}
	return strings.TrimSpace(source.Name)
}

func cloneStateParameter(item *plan.State) *state.Parameter {
	if item == nil {
		return nil
	}
	param := item.Parameter
	if param.In != nil {
		in := *param.In
		param.In = &in
	}
	if param.Schema != nil {
		schema := *param.Schema
		param.Schema = &schema
	}
	if len(param.Predicates) > 0 {
		preds := make([]*extension.PredicateConfig, 0, len(param.Predicates))
		for _, candidate := range param.Predicates {
			if candidate == nil {
				continue
			}
			pred := *candidate
			if len(candidate.Args) > 0 {
				pred.Args = append([]string{}, candidate.Args...)
			}
			preds = append(preds, &pred)
		}
		param.Predicates = preds
	}
	return &param
}

func clonePlanState(item *plan.State) *plan.State {
	if item == nil {
		return nil
	}
	cloned := *item
	if param := cloneStateParameter(item); param != nil {
		cloned.Parameter = *param
	}
	return &cloned
}

func normalizeDerivedInputSchema(param *state.Parameter, resource *view.Resource) {
	if param == nil || param.In == nil || resource == nil {
		return
	}
	if param.In.Kind != state.KindView {
		return
	}
	viewName := strings.TrimSpace(param.Name)
	if name := strings.TrimSpace(param.In.Name); name != "" {
		viewName = name
	}
	aView, _ := resource.View(viewName)
	if aView == nil || aView.Schema == nil {
		return
	}
	required := param.Required != nil && *param.Required
	if param.Schema == nil {
		param.Schema = aView.Schema.Clone()
		if required && param.Schema != nil {
			param.Schema.Cardinality = state.One
		}
		return
	}
	if strings.TrimSpace(param.Schema.Name) == "" {
		param.Schema.Name = strings.TrimSpace(aView.Schema.Name)
	}
	dataType := strings.TrimSpace(param.Schema.DataType)
	if dataType == "" || dataType == "?" || dataType == "interface{}" || dataType == "[]interface{}" || dataType == "*interface{}" || dataType == "string" || dataType == "[]string" {
		param.Schema.DataType = strings.TrimSpace(aView.Schema.DataType)
		if param.Schema.DataType == "" && param.Schema.Name != "" {
			param.Schema.DataType = "*" + param.Schema.Name
		}
	}
	if strings.TrimSpace(param.Schema.Package) == "" {
		param.Schema.Package = strings.TrimSpace(aView.Schema.Package)
	}
	if param.Schema.Type() == nil && aView.Schema.Type() != nil {
		param.Schema.SetType(aView.Schema.Type())
	}
	if param.Schema.Cardinality == "" {
		if required {
			param.Schema.Cardinality = state.One
		} else if aView.Schema.Cardinality != "" {
			param.Schema.Cardinality = aView.Schema.Cardinality
		}
	}
}

func rootResourceView(resource *view.Resource, planned []*plan.View) *view.View {
	if resource == nil {
		return nil
	}
	rootPlan := pickRootView(planned)
	if rootPlan == nil || strings.TrimSpace(rootPlan.Name) == "" {
		if len(resource.Views) > 0 {
			return resource.Views[0]
		}
		return nil
	}
	index := resource.Views.Index()
	root, _ := index.Lookup(rootPlan.Name)
	return root
}

func inheritRootOutputSchema(param *state.Parameter, root *view.View) {
	if param == nil || param.In == nil || root == nil || root.Schema == nil {
		return
	}
	if param.In.Kind != state.KindOutput || !strings.EqualFold(strings.TrimSpace(param.In.Name), "view") {
		return
	}
	dataType := ""
	if param.Schema != nil {
		dataType = strings.TrimSpace(param.Schema.DataType)
	}
	if dataType != "" && dataType != "?" {
		return
	}
	if param.Schema == nil {
		param.Schema = &state.Schema{}
	}
	explicit := *param.Schema
	schema := *root.Schema
	if strings.TrimSpace(explicit.Name) != "" {
		schema.Name = strings.TrimSpace(explicit.Name)
	}
	if dataType := strings.TrimSpace(explicit.DataType); dataType != "" && dataType != "?" {
		schema.DataType = dataType
	}
	if pkg := strings.TrimSpace(explicit.Package); pkg != "" {
		schema.Package = pkg
	}
	if pkgPath := strings.TrimSpace(explicit.PackagePath); pkgPath != "" {
		schema.PackagePath = pkgPath
	}
	if modulePath := strings.TrimSpace(explicit.ModulePath); modulePath != "" {
		schema.ModulePath = modulePath
	}
	if explicit.Cardinality != "" {
		schema.Cardinality = explicit.Cardinality
	}
	param.Schema = &schema
}

func inheritRootBodySchema(param *state.Parameter, root *view.View) {
	if param == nil || param.In == nil || root == nil || root.Schema == nil {
		return
	}
	if param.In.Kind != state.KindRequestBody {
		return
	}
	if !param.IsAnonymous() {
		return
	}
	dataType := ""
	if param.Schema != nil {
		dataType = strings.TrimSpace(param.Schema.DataType)
	}
	if dataType != "" && dataType != "?" {
		return
	}
	if param.Schema == nil {
		param.Schema = &state.Schema{}
	}
	explicit := *param.Schema
	schema := *root.Schema
	if strings.TrimSpace(explicit.Name) != "" {
		schema.Name = strings.TrimSpace(explicit.Name)
	}
	if dataType := strings.TrimSpace(explicit.DataType); dataType != "" && dataType != "?" {
		schema.DataType = dataType
	}
	if pkg := strings.TrimSpace(explicit.Package); pkg != "" {
		schema.Package = pkg
	}
	if pkgPath := strings.TrimSpace(explicit.PackagePath); pkgPath != "" {
		schema.PackagePath = pkgPath
	}
	if modulePath := strings.TrimSpace(explicit.ModulePath); modulePath != "" {
		schema.ModulePath = modulePath
	}
	if explicit.Cardinality != "" {
		schema.Cardinality = explicit.Cardinality
	}
	param.Schema = &schema
}

func ensureMaterializedOutputSchema(param *state.Parameter, root *view.View) {
	if param == nil || param.In == nil {
		return
	}
	if param.In.Kind != state.KindOutput {
		return
	}
	if param.Schema != nil && (param.Schema.Type() != nil || strings.TrimSpace(param.Schema.DataType) != "") {
		return
	}
	switch strings.ToLower(strings.TrimSpace(param.In.Name)) {
	case "status":
		param.Schema = state.NewSchema(reflect.TypeOf(response.Status{}))
	case "summary":
		if root != nil && root.Template != nil && root.Template.Summary != nil && root.Template.Summary.Schema != nil {
			param.Schema = root.Template.Summary.Schema.Clone()
		}
	}
}
