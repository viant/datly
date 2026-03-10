package load

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

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
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/tagly/format/text"
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
	pResult, resource, err := l.materialize(ctx, planned, loadOptions)
	if err != nil {
		return nil, err
	}
	if len(pResult.Views) == 0 {
		return nil, ErrEmptyViewPlan
	}
	return &shape.ViewArtifacts{Resource: resource, Views: resource.Views}, nil
}

// LoadResource implements shape.Loader.
func (l *Loader) LoadResource(ctx context.Context, planned *shape.PlanResult, opts ...shape.LoadOption) (*shape.ResourceArtifacts, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	loadOptions := &shape.LoadOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(loadOptions)
		}
	}
	_, resource, err := l.materialize(ctx, planned, loadOptions)
	if err != nil {
		return nil, err
	}
	return &shape.ResourceArtifacts{Resource: resource}, nil
}

// LoadComponent implements shape.Loader.
func (l *Loader) LoadComponent(ctx context.Context, planned *shape.PlanResult, opts ...shape.LoadOption) (*shape.ComponentArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	loadOptions := &shape.LoadOptions{UseTypeContextPackages: true}
	for _, opt := range opts {
		if opt != nil {
			opt(loadOptions)
		}
	}
	pResult, resource, err := l.materialize(ctx, planned, loadOptions)
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

func (l *Loader) materialize(ctx context.Context, planned *shape.PlanResult, loadOptions *shape.LoadOptions) (*plan.Result, *view.Resource, error) {
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
	materializeConcreteViewSchemas(resource, planned.Source, pResult.TypeContext)
	refineViewColumnConfigTypes(resource, planned.Source, pResult.TypeContext)
	enrichConcreteViewColumns(resource)
	assignViewSummarySchemas(resource, pResult, planned.Source)
	enrichRelationLinkFields(pResult.Views)
	attachViewRelations(resource, pResult.Views)
	if err := enrichRelationHolderTypes(resource, pResult.Views); err != nil {
		return nil, nil, err
	}
	refineSummarySchemas(resource)
	applyVeltyAliasesToExecInputViews(resource, pResult)
	materializeResourceTypes(resource, pResult.Views, planned.Source, pResult.TypeContext)
	applyVeltyAliasesToExecInputViews(resource, pResult)
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
			inheritRootBodySchema(param, rootView)
		}
		if rootView != nil {
			inheritRootOutputSchema(param, rootView)
		}
		ensureMaterializedOutputSchema(param, rootView, planned.Source, pResult.TypeContext)
		addResourceParameter(resource, param)
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
			addResourceParameter(resource, constParam)
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
	applyMutableRootMode(component, resource)
	applyStateBuckets(component, pResult.States, resource, source, pResult.TypeContext, loadOptions)
	applyStateBuckets(component, synthesizeConstStates(pResult.Const), resource, source, pResult.TypeContext, loadOptions)
	applyStateBuckets(component, synthesizeMissingRouteContractStates(component, pResult.Components), resource, source, pResult.TypeContext, loadOptions)
	synthesizeMutableExecHelpers(component, resource)
	component.Input = append(component.Input, synthesizePredicateStates(component.Input, component.Predicates)...)
	component.Directives = cloneDirectives(pResult.Directives)
	if primary := firstComponentRoute(pResult.Components); primary != nil && primary.Report != nil {
		component.Report = &dqlshape.ReportDirective{
			Enabled:    primary.Report.Enabled,
			Input:      strings.TrimSpace(primary.Report.Input),
			Dimensions: strings.TrimSpace(primary.Report.Dimensions),
			Measures:   strings.TrimSpace(primary.Report.Measures),
			Filters:    strings.TrimSpace(primary.Report.Filters),
			OrderBy:    strings.TrimSpace(primary.Report.OrderBy),
			Limit:      strings.TrimSpace(primary.Report.Limit),
			Offset:     strings.TrimSpace(primary.Report.Offset),
		}
	} else if component.Directives != nil && component.Directives.Report != nil {
		component.Report = &dqlshape.ReportDirective{
			Enabled:    component.Directives.Report.Enabled,
			Input:      strings.TrimSpace(component.Directives.Report.Input),
			Dimensions: strings.TrimSpace(component.Directives.Report.Dimensions),
			Measures:   strings.TrimSpace(component.Directives.Report.Measures),
			Filters:    strings.TrimSpace(component.Directives.Report.Filters),
			OrderBy:    strings.TrimSpace(component.Directives.Report.OrderBy),
			Limit:      strings.TrimSpace(component.Directives.Report.Limit),
			Offset:     strings.TrimSpace(component.Directives.Report.Offset),
		}
	}
	component.ColumnsDiscovery = pResult.ColumnsDiscovery
	component.TypeSpecs = resolveTypeSpecs(pResult)
	return component
}

func addResourceParameter(resource *view.Resource, param *state.Parameter) {
	if resource == nil || param == nil {
		return
	}
	resource.AddParameters(param)
	if named := resource.NamedParameters(); named != nil {
		_ = named.Register(param)
	}
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

func applyMutableRootMode(component *Component, resource *view.Resource) {
	if component == nil || resource == nil {
		return
	}
	if strings.EqualFold(strings.TrimSpace(component.Method), "GET") {
		return
	}
	rootView := lookupNamedResourceView(resource, component.RootView)
	if rootView == nil {
		return
	}
	if rootView.Mode != view.ModeHandler {
		rootView.Mode = view.ModeExec
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

func synthesizeMutableExecHelpers(component *Component, resource *view.Resource) {
	if component == nil || resource == nil {
		return
	}
	if strings.EqualFold(strings.TrimSpace(component.Method), "GET") {
		return
	}
	if templateType := strings.ToLower(strings.TrimSpace(componentTemplateType(component))); templateType != "" && templateType != "patch" {
		return
	}
	rootView := lookupNamedResourceView(resource, component.RootView)
	if rootView == nil || rootView.Schema == nil || rootView.Mode != view.ModeExec {
		return
	}
	_ = view.WithTemplateParameterStateType(true)(rootView)
	body := firstMutableBodyState(component.Input)
	if body == nil || body.In == nil || body.Schema == nil {
		return
	}
	bodyName := strings.TrimSpace(body.Name)
	if bodyName == "" {
		return
	}
	helperViewName := "Cur" + bodyName
	if hasInputState(component.Input, helperViewName) {
		return
	}
	keyFieldName, keyColumnName, keyType := mutableKeyDescriptor(rootView, body.Schema)
	if keyFieldName == "" || keyColumnName == "" || keyType == nil {
		return
	}
	componentDir := text.CaseFormatUpperCamel.Format(strings.TrimSpace(componentRootName(component, rootView, bodyName)), text.CaseFormatLowerUnderscore)
	if componentDir == "" {
		componentDir = text.CaseFormatUpperCamel.Format(bodyName, text.CaseFormatLowerUnderscore)
	}
	helperIDsName := helperViewName + keyFieldName
	helperViewURI := path.Join(componentDir, text.CaseFormatUpperCamel.Format(helperViewName, text.CaseFormatLowerUnderscore)+".sql")

	valuesType := reflect.StructOf([]reflect.StructField{{
		Name: "Values",
		Type: reflect.SliceOf(keyType),
		Tag:  reflect.StructTag(`json:",omitempty"`),
	}})
	helperIDsSchema := state.NewSchema(reflect.PtrTo(valuesType))
	if helperIDsSchema != nil && strings.TrimSpace(helperIDsSchema.DataType) == "" {
		helperIDsSchema.DataType = loaderSchemaTypeExpr(reflect.PtrTo(valuesType))
	}
	helperSourceSchema := body.Schema
	if helperSourceSchema == nil && rootView.Schema != nil {
		helperSourceSchema = rootView.Schema.Clone()
	}
	if helperSourceSchema != nil {
		helperSourceSchema = helperSourceSchema.Clone()
	}
	helperIDsParam := &state.Parameter{
		Name:           helperIDsName,
		In:             state.NewParameterLocation(bodyName),
		Schema:         helperSourceSchema,
		Output:         &state.Codec{Name: "structql", Body: fmt.Sprintf(" SELECT ARRAY_AGG(%s) AS Values FROM  `/` LIMIT 1", keyFieldName), Schema: helperIDsSchema.Clone()},
		PreserveSchema: true,
	}
	resource.Parameters.Append(helperIDsParam)

	helperSchema := rootView.Schema.Clone()
	helperSchema.Cardinality = state.Many
	helperViewParamSchema := helperSchema.Clone()
	if helperViewParamSchema.Cardinality == "" {
		helperViewParamSchema.Cardinality = state.Many
	}
	helperViewParam := &state.Parameter{
		Name:   helperViewName,
		In:     state.NewViewLocation(helperViewName),
		Tag:    fmt.Sprintf(`view:"%s" sql:"uri=%s"`, helperViewName, helperViewURI),
		Schema: helperViewParamSchema,
	}
	resource.Parameters.Append(helperViewParam)
	bindViewTemplateParameters(rootView, []*state.Parameter{
		helperIDsParam,
		helperViewParam,
	})

	helperView := view.NewView(helperViewName, "", view.WithSchema(helperSchema.Clone()), view.WithMode(view.ModeQuery))
	helperView.Table = rootView.Table
	helperView.Connector = rootView.Connector
	helperView.Columns = cloneViewColumns(rootView.Columns)
	helperView.ColumnsConfig = cloneViewColumnsConfig(rootView.ColumnsConfig)
	helperView.Selector = &view.Config{
		Namespace: strings.ToLower(truncateString(helperViewName, 2)),
		Limit:     1000,
		Constraints: &view.Constraints{
			Criteria:   true,
			Limit:      true,
			Offset:     true,
			Projection: true,
		},
	}
	helperView.Template = view.NewTemplate(
		fmt.Sprintf("SELECT * FROM %s\nWHERE $criteria.In(%q, $Unsafe.%s.Values)", rootView.Table, keyColumnName, helperIDsName),
		view.WithTemplateParameters(helperIDsParam),
		view.WithTemplateUnsafeStateFromParameters(true),
		view.WithTemplateDeclaredParametersOnly(true),
		view.WithTemplateResourceParameterLookup(true),
	)
	helperView.Template.SourceURL = helperViewURI
	resource.AddViews(helperView)
	component.Views = append(component.Views, helperViewName)
	synthesizeMutableRootTemplate(component, rootView, body, keyFieldName, helperViewName)
}

func componentTemplateType(component *Component) string {
	if component == nil || component.Directives == nil {
		return ""
	}
	return strings.TrimSpace(component.Directives.TemplateType)
}

func synthesizeMutableRootTemplate(component *Component, rootView *view.View, body *plan.State, keyFieldName string, helperViewName string) {
	if component == nil || rootView == nil || body == nil || body.Schema == nil {
		return
	}
	method := strings.ToUpper(strings.TrimSpace(component.Method))
	switch method {
	case "PATCH", "POST", "PUT":
	default:
		return
	}
	bodyName := strings.TrimSpace(body.Name)
	if bodyName == "" || strings.TrimSpace(rootView.Table) == "" || strings.TrimSpace(keyFieldName) == "" {
		return
	}
	if rootView.Template == nil {
		rootView.Template = view.NewTemplate("", view.WithTemplateParameters())
	}
	if rootView.TableBatches == nil {
		rootView.TableBatches = map[string]bool{}
	}
	rootView.TableBatches[rootView.Table] = true
	rootView.Template.Source = buildMutableRootTemplate(method, rootView.Table, bodyName, keyFieldName, helperViewName, body.Schema.Cardinality == state.Many)
}

func buildMutableRootTemplate(method string, tableName string, bodyName string, keyFieldName string, helperViewName string, many bool) string {
	var builder strings.Builder
	if strings.ToUpper(strings.TrimSpace(method)) != "PUT" {
		builder.WriteString(fmt.Sprintf("$sequencer.Allocate(%q, $Unsafe.%s, %q)\n\n", tableName, bodyName, keyFieldName))
	}
	mapName := helperViewName + "By" + keyFieldName
	builder.WriteString(fmt.Sprintf("#set($%s = $Unsafe.%s.IndexBy(%q))\n\n", mapName, helperViewName, keyFieldName))
	if many {
		recordVar := "Rec" + bodyName
		builder.WriteString(fmt.Sprintf("#foreach($%s in $Unsafe.%s)\n", recordVar, bodyName))
		builder.WriteString(fmt.Sprintf("  #if($%s.HasKey($%s.%s) == true)\n", mapName, recordVar, keyFieldName))
		builder.WriteString(fmt.Sprintf("$sql.Update($%s, %q);\n", recordVar, tableName))
		builder.WriteString("  #else\n")
		builder.WriteString(fmt.Sprintf("$sql.Insert($%s, %q);\n", recordVar, tableName))
		builder.WriteString("  #end\n")
		builder.WriteString("#end")
		return builder.String()
	}
	builder.WriteString(fmt.Sprintf("#if($Unsafe.%s)\n", bodyName))
	builder.WriteString(fmt.Sprintf("  #if($%s.HasKey($Unsafe.%s.%s) == true)\n", mapName, bodyName, keyFieldName))
	builder.WriteString(fmt.Sprintf("$sql.Update($Unsafe.%s, %q);\n", bodyName, tableName))
	builder.WriteString("  #else\n")
	builder.WriteString(fmt.Sprintf("$sql.Insert($Unsafe.%s, %q);\n", bodyName, tableName))
	builder.WriteString("  #end\n")
	builder.WriteString("#end")
	return builder.String()
}

func loaderSchemaTypeExpr(rType reflect.Type) string {
	if rType == nil {
		return ""
	}
	switch rType.Kind() {
	case reflect.Ptr:
		return "*" + loaderSchemaTypeExpr(rType.Elem())
	case reflect.Slice:
		return "[]" + loaderSchemaTypeExpr(rType.Elem())
	case reflect.Array:
		return fmt.Sprintf("[%d]%s", rType.Len(), loaderSchemaTypeExpr(rType.Elem()))
	case reflect.Map:
		return "map[" + loaderSchemaTypeExpr(rType.Key()) + "]" + loaderSchemaTypeExpr(rType.Elem())
	default:
		return rType.String()
	}
}

func firstMutableBodyState(states []*plan.State) *plan.State {
	for _, item := range states {
		if item == nil || item.In == nil || item.In.Kind != state.KindRequestBody {
			continue
		}
		if !item.IsAnonymous() {
			continue
		}
		return item
	}
	return nil
}

func hasInputState(states []*plan.State, name string) bool {
	for _, item := range states {
		if item == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(item.Name), strings.TrimSpace(name)) {
			return true
		}
	}
	return false
}

func componentRootName(component *Component, rootView *view.View, fallback string) string {
	if rootView != nil && strings.TrimSpace(rootView.Name) != "" {
		return rootView.Name
	}
	if component != nil && strings.TrimSpace(component.RootView) != "" {
		return component.RootView
	}
	return fallback
}

func mutableKeyDescriptor(rootView *view.View, schema *state.Schema) (string, string, reflect.Type) {
	if fieldName, columnName, rType := mutableKeyFromType(schema); fieldName != "" && columnName != "" && rType != nil {
		return fieldName, columnName, rType
	}
	if rootView == nil {
		return "", "", nil
	}
	for _, column := range rootView.Columns {
		if column == nil {
			continue
		}
		dbColumn := strings.TrimSpace(column.DatabaseColumn)
		if dbColumn == "" {
			dbColumn = strings.TrimSpace(column.Name)
		}
		if !strings.EqualFold(dbColumn, "ID") {
			continue
		}
		fieldName := strings.TrimSpace(column.FieldName())
		if fieldName == "" {
			fieldName = "Id"
		}
		switch strings.ToLower(strings.TrimSpace(column.DataType)) {
		case "int", "integer", "bigint", "smallint":
			return fieldName, dbColumn, reflect.TypeOf(0)
		}
	}
	return "", "", nil
}

func mutableKeyFromType(schema *state.Schema) (string, string, reflect.Type) {
	if schema == nil || schema.Type() == nil {
		return "", "", nil
	}
	rType := schema.Type()
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return "", "", nil
	}
	if field, ok := rType.FieldByName("Id"); ok {
		return "Id", "ID", derefType(field.Type)
	}
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		sqlxTag := field.Tag.Get("sqlx")
		if sqlxTag == "ID" || strings.Contains(sqlxTag, "name=ID") {
			return field.Name, "ID", derefType(field.Type)
		}
	}
	return "", "", nil
}

func derefType(rType reflect.Type) reflect.Type {
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}

func truncateString(value string, max int) string {
	value = strings.TrimSpace(value)
	if max <= 0 || len(value) <= max {
		return value
	}
	return value[:max]
}

func cloneViewColumns(columns []*view.Column) []*view.Column {
	if len(columns) == 0 {
		return nil
	}
	result := make([]*view.Column, 0, len(columns))
	for _, item := range columns {
		if item == nil {
			continue
		}
		cloned := *item
		result = append(result, &cloned)
	}
	return result
}

func cloneViewColumnsConfig(columns map[string]*view.ColumnConfig) map[string]*view.ColumnConfig {
	if len(columns) == 0 {
		return nil
	}
	result := make(map[string]*view.ColumnConfig, len(columns))
	for key, item := range columns {
		if item == nil {
			continue
		}
		cloned := *item
		result[key] = &cloned
	}
	return result
}

func lookupNamedResourceView(resource *view.Resource, name string) *view.View {
	if resource == nil {
		return nil
	}
	if strings.TrimSpace(name) != "" {
		for _, item := range resource.Views {
			if item != nil && strings.EqualFold(strings.TrimSpace(item.Name), strings.TrimSpace(name)) {
				return item
			}
		}
	}
	for _, item := range resource.Views {
		if item != nil {
			return item
		}
	}
	return nil
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
func applyStateBuckets(component *Component, states []*plan.State, resource *view.Resource, source *shape.Source, ctx *typectx.Context, loadOptions *shape.LoadOptions) {
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
		if selector := strings.TrimSpace(cloned.QuerySelector); selector != "" {
			if component.QuerySelectors == nil {
				component.QuerySelectors = map[string][]string{}
			}
			component.QuerySelectors[selector] = append(component.QuerySelectors[selector], cloned.Name)
		}
		normalizeDerivedInputSchema(&cloned.Parameter, resource)
		inheritRootBodySchema(&cloned.Parameter, rootResourceView(resource, nil))
		inheritRootOutputSchema(&cloned.Parameter, rootResourceView(resource, nil))
		ensureMaterializedOutputSchema(&cloned.Parameter, rootResourceView(resource, nil), source, ctx)
		kind := state.Kind(strings.ToLower(item.KindString()))
		inName := item.InName()
		if kind == "" && inName == "" {
			component.Other = append(component.Other, cloned)
			continue
		}
		if cloned.EmitOutput && kind != state.KindOutput {
			outputClone := clonePlanState(cloned)
			if outputClone != nil {
				component.Output = append(component.Output, outputClone)
			}
		}
		switch kind {
		case state.KindQuery, state.KindPath, state.KindHeader, state.KindRequestBody,
			state.KindView, state.KindComponent, state.KindConst,
			state.KindForm, state.KindCookie, state.KindRequest, "":
			if kind == state.KindComponent {
				normalizeDynamicComponentSchema(&cloned.Parameter)
			}
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

func normalizeDynamicComponentSchema(param *state.Parameter) {
	if param == nil || param.Schema == nil {
		return
	}
	param.Schema.SetType(reflect.TypeOf((*interface{})(nil)).Elem())
	param.Schema.Package = ""
	param.Schema.PackagePath = ""
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
	if !shouldInheritTypeContextPackage(typeName) {
		return
	}
	if _, err := types.LookupType(nil, typeName); err == nil {
		return
	}
	if baseType := schemaBaseTypeName(typeName); baseType != typeName {
		if _, err := types.LookupType(nil, baseType); err == nil {
			return
		}
	}
	pkg, pkgPath := schemaTypeContextPackage(component.TypeContext)
	if pkgPath == "" {
		return
	}
	if pkg != "" {
		param.Schema.Package = pkg
	}
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
	if !shouldInheritTypeContextPackage(typeName) {
		return
	}
	if _, err := types.LookupType(nil, typeName); err == nil {
		return
	}
	if baseType := schemaBaseTypeName(typeName); baseType != typeName {
		if _, err := types.LookupType(nil, baseType); err == nil {
			return
		}
	}
	pkg, pkgPath := schemaTypeContextPackage(ctx)
	if pkgPath == "" {
		return
	}
	if pkg != "" {
		aView.Schema.Package = pkg
	}
	aView.Schema.PackagePath = pkgPath
}

func schemaTypeContextPackage(ctx *typectx.Context) (string, string) {
	if ctx == nil {
		return "", ""
	}
	pkg := strings.TrimSpace(ctx.PackageName)
	pkgPath := strings.TrimSpace(ctx.PackagePath)
	if pkgPath == "" {
		pkgPath = strings.TrimSpace(ctx.DefaultPackage)
	}
	if pkg == "" && pkgPath != "" {
		pkg = path.Base(pkgPath)
	}
	return pkg, pkgPath
}

func shouldInheritTypeContextPackage(typeName string) bool {
	baseType := schemaBaseTypeName(typeName)
	if baseType == "" {
		return false
	}
	if strings.Contains(baseType, ".") {
		return false
	}
	if builtinSchemaTypes[baseType] {
		return false
	}
	return true
}

func schemaBaseTypeName(typeName string) string {
	typeName = strings.TrimSpace(typeName)
	for {
		switch {
		case strings.HasPrefix(typeName, "[]"):
			typeName = strings.TrimSpace(typeName[2:])
		case strings.HasPrefix(typeName, "*"):
			typeName = strings.TrimSpace(typeName[1:])
		default:
			goto done
		}
	}
done:
	if typeName == "" {
		return ""
	}
	if strings.ContainsAny(typeName, " {}[](),") {
		return ""
	}
	return typeName
}

var builtinSchemaTypes = map[string]bool{
	"any":         true,
	"bool":        true,
	"byte":        true,
	"complex128":  true,
	"complex64":   true,
	"error":       true,
	"float32":     true,
	"float64":     true,
	"int":         true,
	"int16":       true,
	"int32":       true,
	"int64":       true,
	"int8":        true,
	"interface{}": true,
	"rune":        true,
	"string":      true,
	"uint":        true,
	"uint16":      true,
	"uint32":      true,
	"uint64":      true,
	"uint8":       true,
	"uintptr":     true,
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
		TemplateType:     strings.TrimSpace(input.TemplateType),
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
	if input.Report != nil {
		ret.Report = &dqlshape.ReportDirective{
			Enabled:    input.Report.Enabled,
			Input:      strings.TrimSpace(input.Report.Input),
			Dimensions: strings.TrimSpace(input.Report.Dimensions),
			Measures:   strings.TrimSpace(input.Report.Measures),
			Filters:    strings.TrimSpace(input.Report.Filters),
			OrderBy:    strings.TrimSpace(input.Report.OrderBy),
			Limit:      strings.TrimSpace(input.Report.Limit),
			Offset:     strings.TrimSpace(input.Report.Offset),
		}
	}
	if ret.Meta == "" && ret.DefaultConnector == "" && ret.TemplateType == "" &&
		ret.Dest == "" && ret.InputDest == "" && ret.OutputDest == "" && ret.RouterDest == "" &&
		ret.InputType == "" && ret.OutputType == "" &&
		ret.Cache == nil && ret.MCP == nil && ret.Route == nil && ret.Report == nil && len(ret.Const) == 0 {
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
	if item.Groupable != nil {
		opts = append(opts, view.WithGroupable(*item.Groupable))
	}

	if item.Connector != "" {
		opts = append(opts, view.WithConnectorRef(item.Connector))
	}
	if item.SQL != "" || item.SQLURI != "" {
		tmpl := view.NewTemplate(item.SQL)
		tmpl.SourceURL = item.SQLURI
		opts = append(opts, view.WithTemplate(tmpl))
	}
	if strings.TrimSpace(item.Summary) != "" || strings.TrimSpace(item.SummaryURL) != "" {
		name := strings.TrimSpace(item.SummaryName)
		if name == "" {
			name = "Summary"
		}
		opts = append(opts, view.WithSummary(&view.TemplateSummary{
			Name:      name,
			Source:    item.Summary,
			SourceURL: item.SummaryURL,
			Kind:      view.MetaKindRecord,
		}))
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
			if cfg.Groupable != nil {
				columnCfg.Groupable = boolPtr(*cfg.Groupable)
			}
		}
	}
	if strings.TrimSpace(item.SelectorNamespace) != "" || item.SelectorNoLimit != nil || item.SelectorLimit != nil ||
		item.SelectorCriteria != nil || item.SelectorProjection != nil || item.SelectorOrderBy != nil ||
		item.SelectorOffset != nil || item.SelectorPage != nil || len(item.SelectorFilterable) > 0 ||
		len(item.SelectorOrderByColumns) > 0 {
		if aView.Selector == nil {
			aView.Selector = &view.Config{}
		}
		if aView.Selector.Constraints == nil {
			aView.Selector.Constraints = &view.Constraints{}
		}
		if strings.TrimSpace(item.SelectorNamespace) != "" {
			aView.Selector.Namespace = strings.TrimSpace(item.SelectorNamespace)
		}
		if item.SelectorNoLimit != nil {
			aView.Selector.NoLimit = *item.SelectorNoLimit
			aView.Selector.Constraints.Limit = true
		}
		if item.SelectorLimit != nil {
			aView.Selector.Limit = *item.SelectorLimit
			aView.Selector.Constraints.Limit = true
		}
		if item.SelectorCriteria != nil || item.SelectorProjection != nil || item.SelectorOrderBy != nil ||
			item.SelectorOffset != nil || item.SelectorPage != nil || len(item.SelectorFilterable) > 0 ||
			len(item.SelectorOrderByColumns) > 0 {
			if item.SelectorCriteria != nil {
				aView.Selector.Constraints.Criteria = *item.SelectorCriteria
			}
			if item.SelectorProjection != nil {
				aView.Selector.Constraints.Projection = *item.SelectorProjection
			}
			if item.SelectorOrderBy != nil {
				aView.Selector.Constraints.OrderBy = *item.SelectorOrderBy
			}
			if item.SelectorOffset != nil {
				aView.Selector.Constraints.Offset = *item.SelectorOffset
			}
			if item.SelectorPage != nil {
				value := *item.SelectorPage
				aView.Selector.Constraints.Page = &value
			}
			if len(item.SelectorFilterable) > 0 {
				aView.Selector.Constraints.Filterable = append([]string(nil), item.SelectorFilterable...)
			}
			if len(item.SelectorOrderByColumns) > 0 {
				aView.Selector.Constraints.OrderByColumn = map[string]string{}
				for key, value := range item.SelectorOrderByColumns {
					aView.Selector.Constraints.OrderByColumn[key] = value
				}
			}
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
	if aView.Schema != nil && aView.Schema.Type() == nil {
		if rowType := synthesizeViewSchemaType(aView); rowType != nil {
			aView.Schema.SetType(rowType)
			aView.Schema.EnsurePointer()
		}
	}
	return aView, nil
}

func assignViewSummarySchemas(resource *view.Resource, pResult *plan.Result, source *shape.Source) {
	if resource == nil || pResult == nil {
		return
	}
	index := resource.Views.Index()
	for _, item := range pResult.Views {
		if item == nil || strings.TrimSpace(item.Summary) == "" {
			continue
		}
		aView, err := index.Lookup(item.Name)
		if err != nil || aView == nil || aView.Template == nil || aView.Template.Summary == nil {
			continue
		}
		if schema := aView.Template.Summary.Schema; schema != nil && (schema.Type() != nil || (strings.TrimSpace(schema.DataType) != "" && strings.TrimSpace(schema.DataType) != "?")) {
			continue
		}
		summaryType := resolveSummarySchemaType(source, pResult.TypeContext, item.SummaryName)
		if summaryType == nil {
			summaryType = inferSummarySchemaType(item)
		}
		if summaryType == nil {
			continue
		}
		aView.Template.Summary.Schema = materializedSummarySchema(summaryType, item.SummaryName, pResult.TypeContext)
	}
}

func inferSummarySchemaType(item *plan.View) reflect.Type {
	if item == nil {
		return nil
	}
	summarySQL := strings.TrimSpace(item.Summary)
	if summarySQL == "" {
		return nil
	}
	queryNode, _, err := pipeline.ParseSelectWithDiagnostic(pipeline.NormalizeParserSQL(summarySQL))
	if err != nil || queryNode == nil {
		return nil
	}
	_, elementType, _ := pipeline.InferProjectionType(queryNode)
	return unwrapSummarySchemaType(elementType)
}

func unwrapSummarySchemaType(rType reflect.Type) reflect.Type {
	for rType != nil {
		switch rType.Kind() {
		case reflect.Slice, reflect.Array, reflect.Ptr:
			rType = rType.Elem()
		default:
			return rType
		}
	}
	return nil
}

func resolveSummarySchemaType(source *shape.Source, ctx *typectx.Context, summaryName string) reflect.Type {
	summaryName = strings.TrimSpace(summaryName)
	if summaryName == "" || source == nil {
		return nil
	}
	registry := source.EnsureTypeRegistry()
	if registry == nil {
		return nil
	}
	candidates := []string{summaryName}
	if !strings.HasSuffix(summaryName, "View") {
		candidates = append([]string{summaryName + "View"}, candidates...)
	}
	resolver := typectx.NewResolver(registry, ctx)
	for _, candidate := range candidates {
		if lookup := registry.Lookup(candidate); lookup != nil && lookup.Type != nil {
			return lookup.Type
		}
		if resolved, err := resolver.Resolve(candidate); err == nil && resolved != "" {
			if lookup := registry.Lookup(resolved); lookup != nil && lookup.Type != nil {
				return lookup.Type
			}
		}
	}
	return nil
}

func resolveViewSchemaType(source *shape.Source, ctx *typectx.Context, aView *view.View, typeName string) reflect.Type {
	candidates := []string{strings.TrimSpace(typeName)}
	if aView != nil && aView.Schema != nil {
		if name := strings.TrimSpace(aView.Schema.Name); name != "" {
			candidates = append([]string{name}, candidates...)
		}
	}
	seen := map[string]bool{}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || seen[candidate] {
			continue
		}
		seen[candidate] = true
		if astType := resolveViewSchemaASTType(source, ctx, aView, candidate); astType != nil {
			return astType
		}
		if source != nil {
			registry := source.EnsureTypeRegistry()
			if registry != nil {
				resolver := typectx.NewResolver(registry, ctx)
				if lookup := registry.Lookup(candidate); lookup != nil && lookup.Type != nil {
					return lookup.Type
				}
				if resolved, err := resolver.Resolve(candidate); err == nil && resolved != "" {
					if lookup := registry.Lookup(resolved); lookup != nil && lookup.Type != nil {
						return lookup.Type
					}
				}
			}
		}
	}
	return nil
}

func materializeConcreteViewSchemas(resource *view.Resource, source *shape.Source, ctx *typectx.Context) {
	if resource == nil {
		return
	}
	visited := map[*view.View]bool{}
	for _, aView := range resource.Views {
		applyConcreteViewSchemaType(aView, source, ctx, visited)
	}
}

func enrichConcreteViewColumns(resource *view.Resource) {
	if resource == nil {
		return
	}
	visited := map[*view.View]bool{}
	for _, aView := range resource.Views {
		enrichViewColumnsFromSchema(aView, visited)
	}
}

func enrichViewColumnsFromSchema(aView *view.View, visited map[*view.View]bool) {
	if aView == nil || visited[aView] {
		return
	}
	visited[aView] = true
	appendMissingColumnsFromSchema(aView)
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		enrichViewColumnsFromSchema(&rel.Of.View, visited)
	}
}

func refineViewColumnConfigTypes(resource *view.Resource, source *shape.Source, ctx *typectx.Context) {
	if resource == nil {
		return
	}
	visited := map[*view.View]bool{}
	for _, aView := range resource.Views {
		refineViewColumnConfigType(aView, source, ctx, visited)
	}
}

func refineViewColumnConfigType(aView *view.View, source *shape.Source, ctx *typectx.Context, visited map[*view.View]bool) {
	if aView == nil || visited[aView] {
		return
	}
	visited[aView] = true
	applyConfiguredColumnTypes(aView, source, ctx)
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		refineViewColumnConfigType(&rel.Of.View, source, ctx, visited)
	}
}

func applyConfiguredColumnTypes(aView *view.View, source *shape.Source, ctx *typectx.Context) {
	if aView == nil || len(aView.ColumnsConfig) == 0 {
		return
	}
	if aView.Schema != nil && aView.Schema.Type() != nil {
		if refined := refineSchemaTypeByColumnConfig(aView.Schema.Type(), aView.ColumnsConfig, source, ctx); refined != nil && refined != aView.Schema.Type() {
			aView.Schema.SetType(refined)
			aView.Schema.EnsurePointer()
		}
	}
	for _, column := range aView.Columns {
		if column == nil {
			continue
		}
		cfg := lookupColumnConfig(aView.ColumnsConfig, column.Name, column.DatabaseColumn, column.FieldName())
		if cfg == nil || strings.TrimSpace(valueOrEmpty(cfg.DataType)) == "" {
			continue
		}
		if resolved := resolveColumnConfigType(strings.TrimSpace(*cfg.DataType), source, ctx); resolved != nil {
			column.DataType = strings.TrimSpace(*cfg.DataType)
			column.SetColumnType(resolved)
		}
	}
}

func refineSchemaTypeByColumnConfig(rType reflect.Type, configs map[string]*view.ColumnConfig, source *shape.Source, ctx *typectx.Context) reflect.Type {
	if rType == nil {
		return nil
	}
	switch rType.Kind() {
	case reflect.Ptr:
		if refined := refineSchemaTypeByColumnConfig(rType.Elem(), configs, source, ctx); refined != nil && refined != rType.Elem() {
			return reflect.PtrTo(refined)
		}
		return rType
	case reflect.Slice:
		if refined := refineSchemaTypeByColumnConfig(rType.Elem(), configs, source, ctx); refined != nil && refined != rType.Elem() {
			return reflect.SliceOf(refined)
		}
		return rType
	case reflect.Array:
		if refined := refineSchemaTypeByColumnConfig(rType.Elem(), configs, source, ctx); refined != nil && refined != rType.Elem() {
			return reflect.ArrayOf(rType.Len(), refined)
		}
		return rType
	case reflect.Struct:
		fields := make([]reflect.StructField, 0, rType.NumField())
		changed := false
		for i := 0; i < rType.NumField(); i++ {
			field := rType.Field(i)
			cfg := lookupColumnConfig(configs, field.Name, summaryLookupName(field))
			if cfg != nil && strings.TrimSpace(valueOrEmpty(cfg.DataType)) != "" {
				if resolved := resolveColumnConfigType(strings.TrimSpace(*cfg.DataType), source, ctx); resolved != nil && resolved != field.Type {
					field.Type = resolved
					changed = true
				}
			}
			fields = append(fields, field)
		}
		if changed {
			return reflect.StructOf(fields)
		}
	}
	return rType
}

func lookupColumnConfig(configs map[string]*view.ColumnConfig, names ...string) *view.ColumnConfig {
	if len(configs) == 0 {
		return nil
	}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if cfg := configs[name]; cfg != nil {
			return cfg
		}
		for key, cfg := range configs {
			if strings.EqualFold(strings.TrimSpace(key), name) {
				return cfg
			}
		}
	}
	return nil
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func resolveColumnConfigType(dataType string, source *shape.Source, ctx *typectx.Context) reflect.Type {
	dataType = strings.TrimSpace(dataType)
	if dataType == "" {
		return nil
	}
	if resolved, err := types.LookupType(extension.Config.Types.Lookup, dataType); err == nil && resolved != nil {
		return resolved
	}
	if source == nil {
		return nil
	}
	registry := source.EnsureTypeRegistry()
	if registry == nil {
		return nil
	}
	resolver := typectx.NewResolver(registry, ctx)
	if lookup := registry.Lookup(dataType); lookup != nil && lookup.Type != nil {
		return lookup.Type
	}
	if resolved, err := resolver.Resolve(dataType); err == nil && resolved != "" {
		if lookup := registry.Lookup(resolved); lookup != nil && lookup.Type != nil {
			return lookup.Type
		}
	}
	return nil
}

func appendMissingColumnsFromSchema(aView *view.View) {
	if aView == nil || aView.Schema == nil || aView.Schema.Type() == nil {
		return
	}
	structType := types.EnsureStruct(aView.Schema.Type())
	if structType == nil || structType.Kind() != reflect.Struct {
		return
	}
	ioColumns, err := sqlxio.StructColumns(structType, "sqlx")
	if err != nil || len(ioColumns) == 0 {
		return
	}
	type columnMeta struct {
		dataType string
		nullable bool
	}
	metadata := map[string]columnMeta{}
	for _, ioColumn := range ioColumns {
		if ioColumn == nil {
			continue
		}
		meta := columnMeta{dataType: columnDataTypeFromScanType(ioColumn.ScanType())}
		meta.nullable, _ = ioColumn.Nullable()
		tagName := ""
		if tag := ioColumn.Tag(); tag != nil {
			tagName = strings.TrimSpace(tag.Name())
		}
		for _, key := range []string{
			strings.ToUpper(strings.TrimSpace(ioColumn.Name())),
			strings.ToUpper(tagName),
		} {
			if key != "" {
				metadata[key] = meta
			}
		}
	}
	for _, column := range aView.Columns {
		if column == nil || strings.TrimSpace(column.DataType) != "" {
			continue
		}
		for _, key := range []string{
			strings.ToUpper(strings.TrimSpace(column.Name)),
			strings.ToUpper(strings.TrimSpace(column.DatabaseColumn)),
			strings.ToUpper(strings.TrimSpace(column.FieldName())),
		} {
			if meta, ok := metadata[key]; ok {
				if meta.dataType != "" {
					column.DataType = meta.dataType
				}
				column.Nullable = meta.nullable
				break
			}
		}
	}
	existing := map[string]bool{}
	for _, column := range aView.Columns {
		if column == nil {
			continue
		}
		for _, key := range []string{
			strings.ToUpper(strings.TrimSpace(column.Name)),
			strings.ToUpper(strings.TrimSpace(column.DatabaseColumn)),
			strings.ToUpper(strings.TrimSpace(column.FieldName())),
		} {
			if key != "" {
				existing[key] = true
			}
		}
	}
	for _, ioColumn := range ioColumns {
		name := strings.TrimSpace(ioColumn.Name())
		if name == "" || existing[strings.ToUpper(name)] {
			continue
		}
		tagValue := ""
		if tag := ioColumn.Tag(); tag != nil {
			tagValue = tag.Raw
			if tag.Ns != "" {
				if strings.HasSuffix(tagValue, `"`) {
					tagValue = strings.TrimRight(tagValue, `"`) + ",ns=" + tag.Ns + `"`
				} else {
					tagValue += `",ns=` + tag.Ns + `"`
				}
			}
		}
		nullable, _ := ioColumn.Nullable()
		column := view.NewColumn(name, ioColumn.DatabaseTypeName(), ioColumn.ScanType(), nullable, view.WithColumnTag(tagValue))
		if stateTag, _ := tags.ParseStateTags(reflect.StructTag(column.Tag), nil); stateTag != nil {
			if stateTag.Format != nil {
				column.FormatTag = stateTag.Format
			}
			if codec := stateTag.Codec; codec != nil {
				column.Codec = &state.Codec{Name: codec.Name, Args: codec.Arguments}
			}
		}
		aView.Columns = append(aView.Columns, column)
		existing[strings.ToUpper(name)] = true
		if dbName := strings.ToUpper(strings.TrimSpace(column.DatabaseColumn)); dbName != "" {
			existing[dbName] = true
		}
	}
}

func columnDataTypeFromScanType(scanType reflect.Type) string {
	if scanType == nil {
		return ""
	}
	if schema := schemaFromReflectType(scanType); schema != nil {
		return strings.TrimSpace(schema.DataType)
	}
	return strings.TrimSpace(scanType.String())
}

func applyConcreteViewSchemaType(aView *view.View, source *shape.Source, ctx *typectx.Context, visited map[*view.View]bool) {
	if aView == nil || visited[aView] {
		return
	}
	visited[aView] = true
	if aView.Schema != nil {
		if resolved := resolveViewSchemaType(source, ctx, aView, relationTypeName(aView)); resolved != nil {
			if resolved.Kind() != reflect.Ptr {
				resolved = reflect.PtrTo(resolved)
			}
			aView.Schema.SetType(resolved)
			aView.Schema.EnsurePointer()
		}
	}
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		applyConcreteViewSchemaType(&rel.Of.View, source, ctx, visited)
	}
}

func resolveViewSchemaASTType(source *shape.Source, ctx *typectx.Context, aView *view.View, typeName string) reflect.Type {
	pkgDir := resolveViewSchemaPackageDir(source, ctx, aView)
	if pkgDir == "" {
		return nil
	}
	return parseNamedStructType(pkgDir, typeName)
}

func resolveViewSchemaPackageDir(source *shape.Source, ctx *typectx.Context, aView *view.View) string {
	if aView != nil && aView.Schema != nil {
		if pkgPath := strings.TrimSpace(firstNonEmpty(aView.Schema.ModulePath, aView.Schema.PackagePath)); pkgPath != "" {
			if dir := resolveTypePackageDirFromSource(pkgPath, ctx, source); dir != "" {
				return dir
			}
		}
	}
	if ctx == nil {
		return ""
	}
	if dir := strings.TrimSpace(ctx.PackageDir); dir != "" {
		resolvedDir := dir
		if filepath.IsAbs(dir) {
			if isUsablePackageDir(dir) {
				return dir
			}
			resolvedDir = dir
		} else if moduleRoot := nearestModuleRoot(source); moduleRoot != "" {
			resolvedDir = filepath.Join(moduleRoot, filepath.FromSlash(dir))
			if isUsablePackageDir(resolvedDir) {
				return resolvedDir
			}
		}
	}
	if pkgPath := strings.TrimSpace(firstNonEmpty(ctx.PackagePath, ctx.DefaultPackage)); pkgPath != "" {
		return resolveTypePackageDirFromSource(pkgPath, ctx, source)
	}
	return ""
}

func isUsablePackageDir(dir string) bool {
	if strings.TrimSpace(dir) == "" {
		return false
	}
	info, err := os.Stat(dir)
	return err == nil && info.IsDir()
}

func resolveTypePackageDirFromSource(pkgPath string, ctx *typectx.Context, source *shape.Source) string {
	if pkgPath == "" {
		return ""
	}
	moduleRoot := nearestModuleRoot(source)
	if moduleRoot == "" {
		if ctx != nil && strings.TrimSpace(ctx.PackagePath) == strings.TrimSpace(pkgPath) {
			if dir := strings.TrimSpace(ctx.PackageDir); dir != "" {
				if filepath.IsAbs(dir) {
					return dir
				}
			}
		}
		return ""
	}
	modulePath := detectModulePath(moduleRoot)
	if modulePath != "" {
		if rel, ok := packagePathRelative(modulePath, pkgPath); ok {
			if rel == "" {
				return moduleRoot
			}
			return filepath.Join(moduleRoot, filepath.FromSlash(rel))
		}
	}
	if ctx != nil && strings.TrimSpace(ctx.PackagePath) == strings.TrimSpace(pkgPath) {
		if dir := strings.TrimSpace(ctx.PackageDir); dir != "" {
			if filepath.IsAbs(dir) {
				return dir
			}
			return filepath.Join(moduleRoot, filepath.FromSlash(dir))
		}
	}
	return ""
}

func detectModulePath(moduleRoot string) string {
	if moduleRoot == "" {
		return ""
	}
	data, err := os.ReadFile(filepath.Join(moduleRoot, "go.mod"))
	if err != nil {
		return ""
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "module ") {
			continue
		}
		return strings.TrimSpace(strings.TrimPrefix(line, "module "))
	}
	return ""
}

func packagePathRelative(modulePath, pkgPath string) (string, bool) {
	modulePath = strings.TrimSpace(modulePath)
	pkgPath = strings.TrimSpace(pkgPath)
	if modulePath == "" || pkgPath == "" {
		return "", false
	}
	if pkgPath == modulePath {
		return "", true
	}
	if !strings.HasPrefix(pkgPath, modulePath+"/") {
		return "", false
	}
	return strings.TrimPrefix(pkgPath, modulePath+"/"), true
}

func nearestModuleRoot(source *shape.Source) string {
	if source == nil || strings.TrimSpace(source.Path) == "" {
		return ""
	}
	current := filepath.Dir(strings.TrimSpace(source.Path))
	for current != "" && current != string(filepath.Separator) && current != "." {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return ""
}

func parseNamedStructType(pkgDir, typeName string) reflect.Type {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, pkgDir, nil, parser.ParseComments)
	if err != nil || len(pkgs) == 0 {
		return nil
	}
	specs := map[string]*ast.TypeSpec{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.TYPE {
					continue
				}
				for _, spec := range gen.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok || typeSpec.Name == nil {
						continue
					}
					specs[typeSpec.Name.Name] = typeSpec
				}
			}
		}
	}
	cache := map[string]reflect.Type{}
	inProgress := map[string]bool{}
	var buildNamed func(name string) reflect.Type
	var buildExpr func(expr ast.Expr) reflect.Type

	buildNamed = func(name string) reflect.Type {
		if cached, ok := cache[name]; ok {
			return cached
		}
		if inProgress[name] {
			return reflect.TypeOf(new(interface{})).Elem()
		}
		spec := specs[name]
		if spec == nil {
			return nil
		}
		inProgress[name] = true
		rType := buildExpr(spec.Type)
		delete(inProgress, name)
		if rType != nil {
			cache[name] = rType
		}
		return rType
	}

	buildExpr = func(expr ast.Expr) reflect.Type {
		switch actual := expr.(type) {
		case *ast.Ident:
			switch actual.Name {
			case "string":
				return reflect.TypeOf("")
			case "bool":
				return reflect.TypeOf(true)
			case "int":
				return reflect.TypeOf(int(0))
			case "int8":
				return reflect.TypeOf(int8(0))
			case "int16":
				return reflect.TypeOf(int16(0))
			case "int32":
				return reflect.TypeOf(int32(0))
			case "int64":
				return reflect.TypeOf(int64(0))
			case "uint":
				return reflect.TypeOf(uint(0))
			case "uint8":
				return reflect.TypeOf(uint8(0))
			case "uint16":
				return reflect.TypeOf(uint16(0))
			case "uint32":
				return reflect.TypeOf(uint32(0))
			case "uint64":
				return reflect.TypeOf(uint64(0))
			case "float32":
				return reflect.TypeOf(float32(0))
			case "float64":
				return reflect.TypeOf(float64(0))
			case "interface{}", "any":
				return reflect.TypeOf(new(interface{})).Elem()
			default:
				return buildNamed(actual.Name)
			}
		case *ast.StarExpr:
			if inner := buildExpr(actual.X); inner != nil {
				return reflect.PtrTo(inner)
			}
		case *ast.ArrayType:
			if actual.Len == nil {
				if inner := buildExpr(actual.Elt); inner != nil {
					return reflect.SliceOf(inner)
				}
			}
		case *ast.MapType:
			key := buildExpr(actual.Key)
			value := buildExpr(actual.Value)
			if key != nil && value != nil {
				return reflect.MapOf(key, value)
			}
		case *ast.InterfaceType:
			return reflect.TypeOf(new(interface{})).Elem()
		case *ast.SelectorExpr:
			if ident, ok := actual.X.(*ast.Ident); ok && actual.Sel != nil {
				if ident.Name == "time" && actual.Sel.Name == "Time" {
					return reflect.TypeOf(time.Time{})
				}
				if resolved, err := types.LookupType(extension.Config.Types.Lookup, ident.Name+"."+actual.Sel.Name); err == nil && resolved != nil {
					return resolved
				}
			}
		case *ast.StructType:
			fields := make([]reflect.StructField, 0, len(actual.Fields.List))
			seen := map[string]bool{}
			for _, field := range actual.Fields.List {
				if field == nil {
					continue
				}
				fieldType := buildExpr(field.Type)
				if fieldType == nil {
					continue
				}
				tag := reflect.StructTag("")
				if field.Tag != nil {
					tag = reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
				}
				if len(field.Names) == 0 {
					if name := exportedEmbeddedFieldName(field.Type); name != "" {
						if seen[name] {
							continue
						}
						seen[name] = true
						fields = append(fields, reflect.StructField{Name: name, Type: fieldType, Tag: tag, Anonymous: true})
					}
					continue
				}
				for _, name := range field.Names {
					if name == nil || !name.IsExported() {
						continue
					}
					if seen[name.Name] {
						continue
					}
					seen[name.Name] = true
					fields = append(fields, reflect.StructField{Name: name.Name, Type: fieldType, Tag: tag})
				}
			}
			if len(fields) > 0 {
				return reflect.StructOf(fields)
			}
		}
		return nil
	}
	return buildNamed(typeName)
}

func exportedEmbeddedFieldName(expr ast.Expr) string {
	switch actual := expr.(type) {
	case *ast.Ident:
		if actual.IsExported() {
			return actual.Name
		}
	case *ast.SelectorExpr:
		if actual.Sel != nil && actual.Sel.IsExported() {
			return actual.Sel.Name
		}
	case *ast.StarExpr:
		return exportedEmbeddedFieldName(actual.X)
	}
	return ""
}

func materializedSummarySchema(summaryType reflect.Type, summaryName string, ctx *typectx.Context) *state.Schema {
	if summaryType == nil {
		return nil
	}
	if summaryType.Kind() != reflect.Ptr {
		summaryType = reflect.PtrTo(summaryType)
	}
	schema := state.NewSchema(summaryType)
	typeName := strings.TrimSpace(summarySchemaName(summaryName))
	if typeName != "" {
		schema.Name = typeName
		if typeExpr, typePkg := summarySchemaTypeRef(typeName, ctx); typeExpr != "" {
			schema.DataType = typeExpr
			schema.Package = typePkg
			if ctx != nil {
				schema.PackagePath = strings.TrimSpace(ctx.PackagePath)
			}
		}
	}
	schema.EnsurePointer()
	return schema
}

func refineSummarySchemas(resource *view.Resource) {
	if resource == nil {
		return
	}
	visited := map[*view.View]bool{}
	for _, aView := range resource.Views {
		refineViewSummarySchemas(aView, visited)
	}
}

func materializeResourceTypes(resource *view.Resource, planned []*plan.View, source *shape.Source, ctx *typectx.Context) {
	if resource == nil {
		return
	}
	seen := map[string]bool{}
	plannedByName := map[string]*plan.View{}
	for _, item := range planned {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		plannedByName[strings.ToLower(strings.TrimSpace(item.Name))] = item
	}
	for _, item := range resource.Types {
		if item == nil {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(item.Name))
		if name == "" {
			continue
		}
		seen[name] = true
	}
	visited := map[*view.View]bool{}
	for _, aView := range resource.Views {
		collectViewTypes(aView, resource, seen, visited, plannedByName, source, ctx)
	}
}

func collectViewTypes(aView *view.View, resource *view.Resource, seen map[string]bool, visited map[*view.View]bool, plannedByName map[string]*plan.View, source *shape.Source, ctx *typectx.Context) {
	if aView == nil || resource == nil || visited[aView] {
		return
	}
	visited[aView] = true
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		collectViewTypes(&rel.Of.View, resource, seen, visited, plannedByName, source, ctx)
	}
	if aView.Template != nil && aView.Template.Summary != nil {
		addSchemaTypeDefinition(resource, aView.Template.Summary.Schema, seen)
	}
	addViewTypeDefinition(resource, aView, seen, plannedByName, source, ctx)
}

func addViewTypeDefinition(resource *view.Resource, aView *view.View, seen map[string]bool, plannedByName map[string]*plan.View, source *shape.Source, ctx *typectx.Context) {
	if resource == nil || aView == nil {
		return
	}
	baseName := strings.TrimSpace(aView.Ref)
	if baseName == "" {
		baseName = strings.TrimSpace(aView.Name)
	}
	typeName := state.SanitizeTypeName(baseName) + "View"
	key := strings.ToLower(typeName)
	if seen[key] {
		return
	}
	def := &view.TypeDefinition{
		Name:       typeName,
		Package:    viewSchemaPackage(aView),
		ModulePath: viewSchemaModulePath(aView),
		Ptr:        viewSchemaPtr(aView),
	}
	fieldNames := map[string]bool{}
	typedFields := collectTypedViewDefinitionFields(aView, plannedByName, source, ctx, typeName)
	if len(typedFields) > 0 {
		for _, field := range typedFields {
			addTypeDefinitionField(def, fieldNames, field)
		}
	} else {
		for _, column := range aView.Columns {
			if field := typeDefinitionFieldFromColumn(aView, column); field != nil {
				addTypeDefinitionField(def, fieldNames, field)
			}
		}
	}
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		if field := typeDefinitionFieldFromRelation(rel); field != nil {
			addTypeDefinitionField(def, fieldNames, field)
		}
		if field := typeDefinitionFieldFromRelationSummary(rel); field != nil {
			addTypeDefinitionField(def, fieldNames, field)
		}
	}
	if len(def.Fields) == 0 {
		return
	}
	resource.Types = append(resource.Types, def)
	seen[key] = true
}

func collectTypedViewDefinitionFields(aView *view.View, plannedByName map[string]*plan.View, source *shape.Source, ctx *typectx.Context, typeName string) []*view.Field {
	var result []*view.Field
	seen := map[string]bool{}
	appendFields := func(fields []*view.Field) {
		for _, field := range fields {
			if field == nil {
				continue
			}
			name := strings.TrimSpace(field.Name)
			if name == "" || seen[name] {
				continue
			}
			seen[name] = true
			result = append(result, field)
		}
	}
	appendFields(typeDefinitionFieldsFromSchema(aView.Schema))
	appendFields(typeDefinitionFieldsFromReflectType(resolveViewSchemaType(source, ctx, aView, typeName)))
	if len(result) == 0 {
		appendFields(typeDefinitionFieldsFromPlannedView(plannedViewFor(aView, plannedByName)))
	}
	return result
}

func addTypeDefinitionField(def *view.TypeDefinition, names map[string]bool, field *view.Field) {
	if def == nil || field == nil {
		return
	}
	name := strings.TrimSpace(field.Name)
	if name == "" || names[name] {
		return
	}
	names[name] = true
	def.AddField(field)
}

func plannedViewFor(aView *view.View, plannedByName map[string]*plan.View) *plan.View {
	if aView == nil || len(plannedByName) == 0 {
		return nil
	}
	for _, key := range []string{strings.TrimSpace(aView.Ref), strings.TrimSpace(aView.Name)} {
		if key == "" {
			continue
		}
		if item := plannedByName[strings.ToLower(key)]; item != nil {
			return item
		}
	}
	return nil
}

func addSchemaTypeDefinition(resource *view.Resource, schema *state.Schema, seen map[string]bool) {
	addSchemaTypeDefinitionWithName(resource, schema, strings.TrimSpace(summarySchemaName(schemaName(schema))), seen)
}

func addSchemaTypeDefinitionWithName(resource *view.Resource, schema *state.Schema, typeName string, seen map[string]bool) {
	if resource == nil || schema == nil {
		return
	}
	name := strings.TrimSpace(typeName)
	if name == "" {
		return
	}
	key := strings.ToLower(name)
	if seen[key] {
		return
	}
	cloned := schema.Clone()
	cloned.Name = name
	if cloned.DataType == "" && cloned.Type() != nil {
		cloned.DataType = cloned.TypeName()
	}
	resource.Types = append(resource.Types, &view.TypeDefinition{
		Name:        name,
		DataType:    strings.TrimSpace(cloned.DataType),
		Cardinality: cloned.Cardinality,
		Package:     strings.TrimSpace(cloned.Package),
		ModulePath:  firstNonEmpty(strings.TrimSpace(cloned.ModulePath), strings.TrimSpace(cloned.PackagePath)),
		Schema:      cloned,
	})
	seen[key] = true
}

func schemaName(schema *state.Schema) string {
	if schema == nil {
		return ""
	}
	return schema.Name
}

func viewSchemaPackage(aView *view.View) string {
	if aView == nil || aView.Schema == nil {
		return ""
	}
	return strings.TrimSpace(aView.Schema.Package)
}

func viewSchemaModulePath(aView *view.View) string {
	if aView == nil || aView.Schema == nil {
		return ""
	}
	return firstNonEmpty(strings.TrimSpace(aView.Schema.ModulePath), strings.TrimSpace(aView.Schema.PackagePath))
}

func viewSchemaPtr(aView *view.View) bool {
	if aView == nil || aView.Schema == nil {
		return false
	}
	if rType := aView.Schema.Type(); rType != nil {
		if rType.Kind() == reflect.Slice {
			rType = rType.Elem()
		}
		return rType.Kind() == reflect.Ptr
	}
	typeName := strings.TrimSpace(firstNonEmpty(aView.Schema.DataType, aView.Schema.Name))
	return strings.HasPrefix(typeName, "*")
}

func typeDefinitionFieldFromColumn(aView *view.View, column *view.Column) *view.Field {
	if column == nil {
		return nil
	}
	fieldName := strings.TrimSpace(column.FieldName())
	if fieldName == "" && column.Field() != nil {
		fieldName = strings.TrimSpace(column.Field().Name)
	}
	if fieldName == "" {
		caseFormat := text.CaseFormatUpperCamel
		if aView != nil && aView.CaseFormat != "" {
			caseFormat = aView.CaseFormat
		}
		fieldName = state.StructFieldName(caseFormat, column.Name)
	}
	if fieldName == "" {
		return nil
	}
	schema := columnFieldSchema(column)
	if schema == nil {
		return nil
	}
	return &view.Field{
		Name:        fieldName,
		Column:      strings.TrimSpace(column.DatabaseColumn),
		FromName:    fieldName,
		Schema:      schema,
		Tag:         strings.TrimSpace(column.Tag),
		Cardinality: schema.Cardinality,
	}
}

func columnFieldSchema(column *view.Column) *state.Schema {
	if column == nil {
		return nil
	}
	if rType := column.ColumnType(); rType != nil {
		return schemaFromReflectType(rType)
	}
	if dataType := strings.TrimSpace(column.DataType); dataType != "" {
		return &state.Schema{DataType: dataType, Cardinality: state.One}
	}
	return nil
}

func typeDefinitionFieldFromRelation(rel *view.Relation) *view.Field {
	if rel == nil || rel.Of == nil {
		return nil
	}
	typeName := relationTypeName(&rel.Of.View)
	if typeName == "" || strings.TrimSpace(rel.Holder) == "" {
		return nil
	}
	schema := relationSchema(&rel.Of.View, typeName, rel.Cardinality)
	return &view.Field{
		Name:        strings.TrimSpace(rel.Holder),
		Schema:      schema,
		Cardinality: rel.Cardinality,
	}
}

func typeDefinitionFieldFromRelationSummary(rel *view.Relation) *view.Field {
	if rel == nil || rel.Of == nil || rel.Of.View.Template == nil || rel.Of.View.Template.Summary == nil || rel.Of.View.Template.Summary.Schema == nil {
		return nil
	}
	name := strings.TrimSpace(rel.Of.View.Template.Summary.Name)
	if name == "" {
		return nil
	}
	schema := rel.Of.View.Template.Summary.Schema.Clone()
	schema.EnsurePointer()
	return &view.Field{
		Name:   name,
		Schema: schema,
		Tag:    `json:",omitempty" yaml:",omitempty" sqlx:"-"`,
	}
}

func relationTypeName(aView *view.View) string {
	if aView == nil {
		return ""
	}
	baseName := strings.TrimSpace(aView.Ref)
	if baseName == "" {
		baseName = strings.TrimSpace(aView.Name)
	}
	if baseName == "" {
		return ""
	}
	return state.SanitizeTypeName(baseName) + "View"
}

func relationSchema(aView *view.View, typeName string, cardinality state.Cardinality) *state.Schema {
	schema := &state.Schema{
		Name:        typeName,
		DataType:    "*" + typeName,
		Cardinality: cardinality,
	}
	if aView != nil && aView.Schema != nil {
		schema.Package = strings.TrimSpace(aView.Schema.Package)
		schema.PackagePath = strings.TrimSpace(aView.Schema.PackagePath)
		schema.ModulePath = firstNonEmpty(strings.TrimSpace(aView.Schema.ModulePath), strings.TrimSpace(aView.Schema.PackagePath))
	}
	return schema
}

func typeDefinitionFieldsFromSchema(schema *state.Schema) []*view.Field {
	if schema == nil || schema.Type() == nil {
		return nil
	}
	rType := schema.Type()
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	result := make([]*view.Field, 0, rType.NumField())
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if !field.IsExported() {
			continue
		}
		result = append(result, &view.Field{
			Name:        field.Name,
			Schema:      schemaFromReflectType(field.Type),
			Tag:         string(field.Tag),
			FromName:    field.Name,
			Cardinality: state.One,
		})
	}
	return result
}

func typeDefinitionFieldsFromPlannedView(item *plan.View) []*view.Field {
	if item == nil {
		return nil
	}
	return typeDefinitionFieldsFromReflectType(bestSchemaType(item))
}

func typeDefinitionFieldsFromReflectType(rType reflect.Type) []*view.Field {
	if rType == nil {
		return nil
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	result := make([]*view.Field, 0, rType.NumField())
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if !field.IsExported() {
			continue
		}
		result = append(result, &view.Field{
			Name:        field.Name,
			Schema:      schemaFromReflectType(field.Type),
			Tag:         string(field.Tag),
			FromName:    field.Name,
			Cardinality: state.One,
		})
	}
	return result
}

func schemaFromReflectType(rType reflect.Type) *state.Schema {
	if rType == nil {
		return nil
	}
	schema := state.NewSchema(rType)
	if schema == nil {
		return nil
	}
	if schema.Name == "" && schema.DataType == "" {
		schema.DataType = rType.String()
		if schema.Cardinality == "" {
			schema.Cardinality = state.One
		}
	}
	return schema
}

func synthesizeViewSchemaType(aView *view.View) reflect.Type {
	return synthesizeViewSchemaTypeWithOptions(aView, false)
}

func synthesizeViewSchemaTypeWithOptions(aView *view.View, includeVelty bool) reflect.Type {
	if aView == nil || len(aView.Columns) == 0 {
		return nil
	}
	fields := make([]reflect.StructField, 0, len(aView.Columns))
	seen := map[string]bool{}
	for _, column := range aView.Columns {
		structField := viewStructFieldFromColumn(aView, column, includeVelty)
		if structField == nil {
			continue
		}
		if seen[structField.Name] {
			continue
		}
		seen[structField.Name] = true
		fields = append(fields, *structField)
	}
	if len(fields) == 0 {
		return nil
	}
	return reflect.PtrTo(reflect.StructOf(fields))
}

func viewStructFieldFromColumn(aView *view.View, column *view.Column, includeVelty bool) *reflect.StructField {
	if column == nil {
		return nil
	}
	schema := columnFieldSchema(column)
	if schema == nil || schema.Type() == nil {
		return nil
	}
	fieldName := strings.TrimSpace(column.FieldName())
	if fieldName == "" && column.Field() != nil {
		fieldName = strings.TrimSpace(column.Field().Name)
	}
	if fieldName == "" {
		caseFormat := text.CaseFormatUpperCamel
		if aView != nil && aView.CaseFormat != "" {
			caseFormat = aView.CaseFormat
		}
		fieldName = state.StructFieldName(caseFormat, column.Name)
	}
	fieldName = strings.TrimSpace(fieldName)
	if fieldName == "" {
		return nil
	}
	tag := strings.TrimSpace(column.Tag)
	sqlxTag := strings.TrimSpace(strings.TrimSpace(column.DatabaseColumn))
	if sqlxTag == "" {
		sqlxTag = strings.TrimSpace(column.Name)
	}
	if sqlxTag != "" && !strings.Contains(tag, `sqlx:"`) {
		if tag != "" {
			tag += " "
		}
		tag += fmt.Sprintf(`sqlx:"%s"`, sqlxTag)
	}
	if includeVelty && !strings.Contains(tag, `velty:"`) {
		veltyNames := []string{column.Name}
		if fieldName != "" && fieldName != column.Name {
			veltyNames = append(veltyNames, fieldName)
		}
		if tag != "" {
			tag += " "
		}
		tag += fmt.Sprintf(`velty:"names=%s"`, strings.Join(veltyNames, "|"))
	}
	return &reflect.StructField{
		Name: fieldName,
		Type: schema.Type(),
		Tag:  reflect.StructTag(tag),
	}
}

func applyVeltyAliasesToExecInputViews(resource *view.Resource, pResult *plan.Result) {
	if resource == nil || pResult == nil || !planUsesVelty(resource, pResult) {
		return
	}
	viewNames := map[string]bool{}
	for _, item := range pResult.States {
		if item == nil || item.In == nil || item.In.Kind != state.KindView {
			continue
		}
		viewName := strings.TrimSpace(item.Name)
		if name := strings.TrimSpace(item.In.Name); name != "" {
			viewName = name
		}
		if viewName == "" {
			continue
		}
		viewNames[strings.ToLower(viewName)] = true
	}
	if len(viewNames) == 0 {
		return
	}
	for _, aView := range resource.Views {
		if aView == nil || aView.Schema == nil {
			continue
		}
		if !viewNames[strings.ToLower(strings.TrimSpace(aView.Name))] &&
			!viewNames[strings.ToLower(strings.TrimSpace(aView.Reference.Ref))] {
			continue
		}
		applyVeltyAliasesToViewColumns(aView)
		if !schemaNeedsVeltyAliases(aView.Schema.Type()) {
			continue
		}
		if rebuilt := synthesizeViewSchemaTypeWithOptions(aView, true); rebuilt != nil {
			aView.Schema.SetType(rebuilt)
			aView.Schema.EnsurePointer()
			continue
		}
		if rebuilt := ensureSchemaTypeVeltyAliases(aView.Schema.Type()); rebuilt != nil {
			aView.Schema.SetType(rebuilt)
		}
	}
}

func planUsesVelty(resource *view.Resource, pResult *plan.Result) bool {
	if pResult == nil {
		return false
	}
	for _, route := range pResult.Components {
		if route == nil {
			continue
		}
		method := strings.ToUpper(strings.TrimSpace(route.Method))
		if method != "" && method != "GET" && strings.TrimSpace(route.Handler) == "" {
			return true
		}
	}
	if resource != nil {
		for _, aView := range resource.Views {
			if aView != nil && aView.Mode == view.ModeExec {
				return true
			}
		}
	}
	return false
}

func schemaNeedsVeltyAliases(rType reflect.Type) bool {
	if rType == nil {
		return true
	}
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < rType.NumField(); i++ {
		if strings.TrimSpace(rType.Field(i).Tag.Get("velty")) != "" {
			return false
		}
	}
	return true
}

func applyVeltyAliasesToViewColumns(aView *view.View) {
	if aView == nil {
		return
	}
	for _, column := range aView.Columns {
		if column == nil || strings.Contains(column.Tag, `velty:"`) {
			continue
		}
		fieldName := strings.TrimSpace(column.FieldName())
		if fieldName == "" && column.Field() != nil {
			fieldName = strings.TrimSpace(column.Field().Name)
		}
		if fieldName == "" {
			caseFormat := text.CaseFormatUpperCamel
			if aView.CaseFormat != "" {
				caseFormat = aView.CaseFormat
			}
			fieldName = state.StructFieldName(caseFormat, column.Name)
		}
		veltyNames := []string{column.Name}
		if fieldName != "" && fieldName != column.Name {
			veltyNames = append(veltyNames, fieldName)
		}
		tag := strings.TrimSpace(column.Tag)
		if tag != "" {
			tag += " "
		}
		tag += fmt.Sprintf(`velty:"names=%s"`, strings.Join(veltyNames, "|"))
		column.Tag = strings.TrimSpace(tag)
	}
}

func ensureSchemaTypeVeltyAliases(rType reflect.Type) reflect.Type {
	if rType == nil {
		return nil
	}
	original := rType
	isSlice := false
	if rType.Kind() == reflect.Slice {
		isSlice = true
		rType = rType.Elem()
	}
	wasPtr := false
	if rType.Kind() == reflect.Ptr {
		wasPtr = true
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	fields := make([]reflect.StructField, 0, rType.NumField())
	changed := false
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		tag := string(field.Tag)
		if strings.TrimSpace(field.Tag.Get("velty")) == "" {
			sqlxName := summaryTagName(field.Tag.Get("sqlx"))
			if sqlxName == "" {
				sqlxName = field.Name
			}
			veltyNames := []string{sqlxName}
			if field.Name != "" && field.Name != sqlxName {
				veltyNames = append(veltyNames, field.Name)
			}
			if strings.TrimSpace(tag) != "" {
				tag += " "
			}
			tag += fmt.Sprintf(`velty:"names=%s"`, strings.Join(veltyNames, "|"))
			changed = true
		}
		field.Tag = reflect.StructTag(strings.TrimSpace(tag))
		fields = append(fields, field)
	}
	if !changed {
		return original
	}
	rebuilt := reflect.StructOf(fields)
	if wasPtr {
		rebuilt = reflect.PtrTo(rebuilt)
	}
	if isSlice {
		rebuilt = reflect.SliceOf(rebuilt)
	}
	return rebuilt
}

func refineViewSummarySchemas(aView *view.View, visited map[*view.View]bool) {
	if aView == nil || visited[aView] {
		return
	}
	visited[aView] = true
	if aView.Template != nil && aView.Template.Summary != nil && aView.Template.Summary.Schema != nil {
		if refined := refineSummarySchemaType(aView.Template.Summary.Schema, aView); refined != nil {
			aView.Template.Summary.Schema = refined
		}
	}
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		refineViewSummarySchemas(&rel.Of.View, visited)
	}
}

func refineSummarySchemaType(summarySchema *state.Schema, ownerView *view.View) *state.Schema {
	if summarySchema == nil || ownerView == nil {
		return nil
	}
	summaryType := summarySchema.Type()
	if summaryType == nil {
		return nil
	}
	if summaryType.Kind() == reflect.Ptr {
		summaryType = summaryType.Elem()
	}
	if summaryType.Kind() != reflect.Struct {
		return nil
	}
	ownerFields := map[string]reflect.StructField{}
	if ownerSchema := ownerView.Schema; ownerSchema != nil && ownerSchema.CompType() != nil {
		ownerType := ownerSchema.CompType()
		if ownerType.Kind() == reflect.Ptr {
			ownerType = ownerType.Elem()
		}
		if ownerType.Kind() == reflect.Struct {
			for i := 0; i < ownerType.NumField(); i++ {
				field := ownerType.Field(i)
				ownerFields[strings.ToUpper(strings.TrimSpace(field.Name))] = field
				if sqlxName := summaryTagName(field.Tag.Get("sqlx")); sqlxName != "" {
					ownerFields[strings.ToUpper(sqlxName)] = field
				}
			}
		}
	}
	for _, column := range ownerView.Columns {
		if column == nil {
			continue
		}
		columnType := summaryColumnType(column)
		if columnType == nil {
			continue
		}
		fieldName := strings.TrimSpace(column.FieldName())
		if fieldName == "" {
			fieldName = strings.TrimSpace(column.Name)
		}
		field := reflect.StructField{Name: fieldName, Type: columnType, Tag: reflect.StructTag(column.Tag)}
		if key := strings.ToUpper(strings.TrimSpace(column.Name)); key != "" {
			if _, ok := ownerFields[key]; ok {
				continue
			}
			ownerFields[key] = field
		}
		if key := strings.ToUpper(strings.TrimSpace(column.DatabaseColumn)); key != "" {
			if _, ok := ownerFields[key]; ok {
				continue
			}
			ownerFields[key] = field
		}
	}
	if len(ownerFields) == 0 {
		return nil
	}
	fields := make([]reflect.StructField, 0, summaryType.NumField())
	changed := false
	for i := 0; i < summaryType.NumField(); i++ {
		field := summaryType.Field(i)
		if ownerField, ok := ownerFields[strings.ToUpper(summaryLookupName(field))]; ok && ownerField.Type != nil && ownerField.Type != field.Type {
			field.Type = ownerField.Type
			changed = true
		}
		fields = append(fields, field)
	}
	if !changed {
		return nil
	}
	refinedType := reflect.StructOf(fields)
	refined := summarySchema.Clone()
	refined.SetType(refinedType)
	return refined
}

func refreshInlineSummarySchemas(ctx context.Context, resource *view.Resource) {
	if resource == nil {
		return
	}
	visited := map[*view.View]bool{}
	for _, aView := range resource.Views {
		refreshViewInlineSummarySchema(ctx, resource, aView, visited)
	}
}

// RefineSummarySchemas reapplies summary schema refinement using current view schema/column metadata.
// This is useful after late column discovery updated view columns post-load.
func RefineSummarySchemas(resource *view.Resource) {
	if resource == nil {
		return
	}
	refineSummarySchemas(resource)
}

func refreshViewInlineSummarySchema(ctx context.Context, resource *view.Resource, aView *view.View, visited map[*view.View]bool) {
	if aView == nil || visited[aView] {
		return
	}
	visited[aView] = true
	if aView.GetResource() == nil {
		aView.SetResource(resource)
	}
	if shouldRefreshInlineSummarySchema(aView) {
		restore := suppressInlineTemplateURLs(aView.Template)
		_ = aView.Template.Init(ctx, resource, aView)
		restore()
	}
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		if rel.Of.View.GetResource() == nil {
			rel.Of.View.SetResource(resource)
		}
		refreshViewInlineSummarySchema(ctx, resource, &rel.Of.View, visited)
	}
}

func shouldRefreshInlineSummarySchema(aView *view.View) bool {
	if aView == nil || aView.Template == nil || aView.Template.Summary == nil {
		return false
	}
	if aView.Connector == nil || strings.TrimSpace(aView.Connector.Ref) == "" {
		return false
	}
	if strings.TrimSpace(aView.Template.Summary.Source) == "" {
		return false
	}
	if strings.TrimSpace(aView.Template.Source) == "" && strings.TrimSpace(aView.Template.SourceURL) == "" {
		return false
	}
	return true
}

func suppressInlineTemplateURLs(tmpl *view.Template) func() {
	if tmpl == nil {
		return func() {}
	}
	sourceURL := tmpl.SourceURL
	summarySourceURL := ""
	if strings.TrimSpace(tmpl.Source) != "" {
		tmpl.SourceURL = ""
	}
	if tmpl.Summary != nil {
		summarySourceURL = tmpl.Summary.SourceURL
		if strings.TrimSpace(tmpl.Summary.Source) != "" {
			tmpl.Summary.SourceURL = ""
		}
	}
	return func() {
		tmpl.SourceURL = sourceURL
		if tmpl.Summary != nil {
			tmpl.Summary.SourceURL = summarySourceURL
		}
	}
}

func summaryColumnType(column *view.Column) reflect.Type {
	if column == nil {
		return nil
	}
	if rType := column.ColumnType(); rType != nil {
		return rType
	}
	switch strings.ToLower(strings.TrimSpace(column.DataType)) {
	case "int", "integer", "smallint", "signed", "int32":
		if column.Nullable {
			return reflect.TypeOf((*int)(nil))
		}
		return reflect.TypeOf(int(0))
	case "int64", "bigint":
		if column.Nullable {
			return reflect.TypeOf((*int64)(nil))
		}
		return reflect.TypeOf(int64(0))
	case "float", "float32", "real":
		if column.Nullable {
			return reflect.TypeOf((*float32)(nil))
		}
		return reflect.TypeOf(float32(0))
	case "float64", "double", "numeric", "decimal":
		if column.Nullable {
			return reflect.TypeOf((*float64)(nil))
		}
		return reflect.TypeOf(float64(0))
	case "bool", "boolean":
		if column.Nullable {
			return reflect.TypeOf((*bool)(nil))
		}
		return reflect.TypeOf(false)
	case "string", "text", "varchar", "char", "uuid", "json", "jsonb", "":
		if column.Nullable {
			return reflect.TypeOf((*string)(nil))
		}
		return reflect.TypeOf("")
	default:
		return nil
	}
}

func summarySchemaName(summaryName string) string {
	summaryName = strings.TrimSpace(summaryName)
	if summaryName == "" {
		return ""
	}
	if strings.HasSuffix(summaryName, "View") {
		return summaryName
	}
	return exportedSummaryTypeName(summaryName) + "View"
}

func summarySchemaTypeRef(typeName string, ctx *typectx.Context) (string, string) {
	typeName = strings.TrimSpace(typeName)
	if typeName == "" {
		return "", ""
	}
	if ctx != nil {
		if pkgAlias := strings.TrimSpace(ctx.PackageName); pkgAlias != "" {
			return "*" + pkgAlias + "." + typeName, pkgAlias
		}
		if pkgPath := strings.TrimSpace(ctx.PackagePath); pkgPath != "" {
			alias := summaryPackageAlias(pkgPath, ctx)
			return "*" + alias + "." + typeName, alias
		}
	}
	return "*" + typeName, ""
}

func summaryPackageAlias(pkgPath string, ctx *typectx.Context) string {
	pkgPath = strings.TrimSpace(pkgPath)
	if pkgPath == "" {
		return ""
	}
	if ctx != nil {
		for _, item := range ctx.Imports {
			if strings.TrimSpace(item.Package) != pkgPath {
				continue
			}
			if alias := strings.TrimSpace(item.Alias); alias != "" {
				return alias
			}
		}
		if strings.TrimSpace(ctx.PackagePath) == pkgPath && strings.TrimSpace(ctx.PackageName) != "" {
			return strings.TrimSpace(ctx.PackageName)
		}
	}
	if index := strings.LastIndex(pkgPath, "/"); index != -1 && index+1 < len(pkgPath) {
		return pkgPath[index+1:]
	}
	return pkgPath
}

func exportedSummaryTypeName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	parts := strings.FieldsFunc(name, func(r rune) bool {
		return r == '_' || r == '-' || r == ' ' || r == '.'
	})
	if len(parts) == 0 {
		return ""
	}
	var b strings.Builder
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]))
		if len(part) > 1 {
			b.WriteString(part[1:])
		}
	}
	return b.String()
}

func summaryLookupName(field reflect.StructField) string {
	if sqlxName := summaryTagName(field.Tag.Get("sqlx")); sqlxName != "" {
		return sqlxName
	}
	return strings.TrimSpace(field.Name)
}

func summaryTagName(tag string) string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return ""
	}
	if strings.HasPrefix(tag, "name=") {
		tag = strings.TrimPrefix(tag, "name=")
	}
	if idx := strings.Index(tag, ","); idx != -1 {
		tag = tag[:idx]
	}
	return strings.TrimSpace(tag)
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
	if rType.Kind() == reflect.Map || rType.Kind() == reflect.Interface {
		return true
	}
	if rType.Kind() == reflect.Struct {
		if cols := inferColumnsFromType(rType); len(cols) > 0 && inferredColumnsArePlaceholders(cols) {
			return true
		}
	}
	return false
}

func bestSchemaType(item *plan.View) reflect.Type {
	if item.FieldType != nil {
		return normalizeViewSchemaReflectType(item, item.FieldType)
	}
	if item.ElementType != nil {
		return normalizeViewSchemaReflectType(item, item.ElementType)
	}
	return nil
}

func normalizeViewSchemaReflectType(item *plan.View, rType reflect.Type) reflect.Type {
	if item == nil || rType == nil {
		return rType
	}
	schemaType := strings.TrimSpace(item.SchemaType)
	if !strings.HasPrefix(schemaType, "*") {
		return rType
	}
	if strings.EqualFold(strings.TrimSpace(item.Cardinality), string(state.Many)) {
		if rType.Kind() == reflect.Slice {
			elem := rType.Elem()
			if elem.Kind() != reflect.Ptr {
				return reflect.SliceOf(reflect.PtrTo(elem))
			}
		}
		return rType
	}
	if rType.Kind() != reflect.Ptr {
		return reflect.PtrTo(rType)
	}
	return rType
}

func stringPtr(value string) *string {
	ret := value
	return &ret
}

func boolPtr(value bool) *bool {
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

func enrichRelationLinkFields(planned []*plan.View) {
	if len(planned) == 0 {
		return
	}
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
		for _, rel := range item.Relations {
			if rel == nil || len(rel.On) == 0 {
				continue
			}
			parentPlan := item
			if parentName := strings.TrimSpace(rel.Parent); parentName != "" {
				if candidate, ok := byName[strings.ToLower(parentName)]; ok && candidate != nil {
					parentPlan = candidate
				}
			}
			refPlan := byName[strings.ToLower(strings.TrimSpace(rel.Ref))]
			for _, link := range rel.On {
				if link == nil {
					continue
				}
				if link.ParentField == "" {
					if field := fieldNameForColumn(parentPlan, link.ParentColumn); field != "" {
						link.ParentField = field
						link.ParentNamespace = ""
					}
				}
				if link.RefField == "" {
					if field := fieldNameForColumn(refPlan, link.RefColumn); field != "" {
						link.RefField = field
						link.RefNamespace = ""
					}
				}
			}
		}
	}
}

func fieldNameForColumn(item *plan.View, column string) string {
	column = strings.TrimSpace(column)
	if column == "" {
		return ""
	}
	fallback := pipeline.ExportedName(column)
	if item == nil {
		return fallback
	}
	rType := bestSchemaType(item)
	if rType == nil {
		return fallback
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return fallback
	}
	normalizedColumn := normalizeRelationColumnName(column)
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if !field.IsExported() || shouldSkipInferredField(field) {
			continue
		}
		candidate := sqlxColumnName(field)
		if candidate == "" {
			candidate = field.Name
		}
		if normalizeRelationColumnName(candidate) == normalizedColumn {
			return field.Name
		}
	}
	return fallback
}

func normalizeRelationColumnName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	name = strings.ReplaceAll(name, "_", "")
	return name
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
			relation.Of.View = cloneRelationView(ref, relation.Of.View)
			parent.With = append(parent.With, relation)
		}
	}
}

func cloneRelationView(ref *view.View, current view.View) view.View {
	if ref == nil {
		return current
	}
	cloned := *ref
	cloned.Ref = ref.Name
	cloned.Name = ""
	if currentName := strings.TrimSpace(current.Name); currentName != "" && !strings.EqualFold(currentName, ref.Name) {
		cloned.Name = current.Name
	}
	if ref.Schema != nil {
		cloned.Schema = ref.Schema.Clone()
	}
	if ref.Template != nil {
		templateCopy := *ref.Template
		if ref.Template.Schema != nil {
			templateCopy.Schema = ref.Template.Schema.Clone()
		}
		if strings.TrimSpace(templateCopy.Source) != "" {
			templateCopy.SourceURL = ""
		}
		if ref.Template.Summary != nil {
			summaryCopy := *ref.Template.Summary
			if ref.Template.Summary.Schema != nil {
				summaryCopy.Schema = ref.Template.Summary.Schema.Clone()
			}
			if strings.TrimSpace(summaryCopy.Source) != "" {
				summaryCopy.SourceURL = ""
			}
			templateCopy.Summary = &summaryCopy
		}
		cloned.Template = &templateCopy
	}
	if cloned.Selector != nil && cloned.Selector.Limit > 0 {
		if cloned.Batch == nil {
			cloned.Batch = &view.Batch{}
		}
		if cloned.Batch.Size == 0 || cloned.Batch.Size > 1 {
			cloned.Batch.Size = 1
		}
	}
	return cloned
}

func hideRelationSummaryLinkFields(relation *view.Relation) {
	if relation == nil || relation.Of == nil {
		return
	}
	child := &relation.Of.View
	if child.Template == nil || child.Template.Summary == nil || child.Template.Summary.Schema == nil {
		return
	}
	hidden := map[string]bool{}
	for _, link := range relation.Of.On {
		if link == nil {
			continue
		}
		if field := normalizeRelationColumnName(link.Field); field != "" {
			hidden[field] = true
		}
		if column := normalizeRelationColumnName(link.Column); column != "" {
			hidden[column] = true
		}
	}
	if len(hidden) == 0 {
		return
	}
	if refined := hideSummarySchemaFields(child.Template.Summary.Schema, hidden); refined != nil {
		child.Template.Summary.Schema = refined
	}
}

func hideSummarySchemaFields(summarySchema *state.Schema, hidden map[string]bool) *state.Schema {
	if summarySchema == nil || len(hidden) == 0 {
		return nil
	}
	summaryType := summarySchema.Type()
	if summaryType == nil {
		return nil
	}
	isPtr := false
	if summaryType.Kind() == reflect.Ptr {
		isPtr = true
		summaryType = summaryType.Elem()
	}
	if summaryType.Kind() != reflect.Struct {
		return nil
	}
	fields := make([]reflect.StructField, 0, summaryType.NumField())
	changed := false
	for i := 0; i < summaryType.NumField(); i++ {
		field := summaryType.Field(i)
		if shouldHideSummaryField(field, hidden) {
			field.Tag = hideSummaryFieldTag(field.Tag)
			changed = true
		}
		fields = append(fields, field)
	}
	if !changed {
		return nil
	}
	refinedType := reflect.StructOf(fields)
	if isPtr {
		refinedType = reflect.PtrTo(refinedType)
	}
	refined := summarySchema.Clone()
	refined.SetType(refinedType)
	return refined
}

func shouldHideSummaryField(field reflect.StructField, hidden map[string]bool) bool {
	if len(hidden) == 0 {
		return false
	}
	candidates := []string{
		normalizeRelationColumnName(field.Name),
		normalizeRelationColumnName(summaryLookupName(field)),
	}
	for _, candidate := range candidates {
		if candidate != "" && hidden[candidate] {
			return true
		}
	}
	return false
}

var structTagPattern = regexp.MustCompile(`([A-Za-z0-9_]+):"([^"]*)"`)

func hideSummaryFieldTag(tag reflect.StructTag) reflect.StructTag {
	values := map[string]string{}
	order := make([]string, 0, 4)
	for _, match := range structTagPattern.FindAllStringSubmatch(string(tag), -1) {
		key := strings.TrimSpace(match[1])
		if key == "" {
			continue
		}
		if _, ok := values[key]; !ok {
			order = append(order, key)
		}
		values[key] = match[2]
	}
	for _, item := range []struct {
		key   string
		value string
	}{
		{key: "internal", value: "true"},
	} {
		if _, ok := values[item.key]; !ok {
			order = append(order, item.key)
		}
		values[item.key] = item.value
	}
	parts := make([]string, 0, len(order))
	for _, key := range order {
		if value, ok := values[key]; ok {
			parts = append(parts, fmt.Sprintf(`%s:%q`, key, value))
		}
	}
	return reflect.StructTag(strings.Join(parts, " "))
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
				parentType := parent.Schema.Type()
				if parentType != nil && parentType.Kind() == reflect.Slice {
					elemType := parentType.Elem()
					if elemType.Kind() == reflect.Ptr {
						parent.Schema.SetType(reflect.SliceOf(reflect.PtrTo(augmented)))
						continue
					}
				}
				parent.Schema.SetType(reflect.SliceOf(augmented))
				continue
			}
			if parentType := parent.Schema.Type(); parentType != nil && parentType.Kind() == reflect.Ptr {
				parent.Schema.SetType(reflect.PtrTo(augmented))
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
		if _, ok := parentType.FieldByName(rel.Holder); !ok && !fieldNameInSlice(fields, rel.Holder) {
			fieldType := relationHolderFieldType(childType, childPlannedCardinality(childName, byName))
			if fieldType != nil {
				fields = append(fields, reflect.StructField{
					Name: rel.Holder,
					Type: fieldType,
					Tag:  reflect.StructTag(buildRelationHolderTag(rel, childView)),
				})
				changed = true
			}
		}
		if summaryField := relationSummaryField(childView); summaryField != nil {
			if _, ok := parentType.FieldByName(summaryField.Name); !ok && !fieldNameInSlice(fields, summaryField.Name) {
				fields = append(fields, *summaryField)
				changed = true
			}
		}
	}
	if !changed {
		return parentType, false, nil
	}
	return reflect.StructOf(fields), true, nil
}

func relationSummaryField(childView *view.View) *reflect.StructField {
	if childView == nil || childView.Template == nil || childView.Template.Summary == nil || childView.Template.Summary.Schema == nil {
		return nil
	}
	fieldType := childView.Template.Summary.Schema.Type()
	if fieldType == nil {
		return nil
	}
	fieldName := strings.TrimSpace(childView.Template.Summary.Name)
	if fieldName == "" {
		return nil
	}
	if fieldType.Kind() == reflect.Struct {
		fieldType = reflect.PtrTo(fieldType)
	}
	return &reflect.StructField{
		Name: fieldName,
		Type: fieldType,
		Tag:  reflect.StructTag(`json:",omitempty" yaml:",omitempty" sqlx:"-"`),
	}
}

func fieldNameInSlice(fields []reflect.StructField, name string) bool {
	for _, field := range fields {
		if field.Name == name {
			return true
		}
	}
	return false
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
		if aView.Template.DeclaredParametersOnly {
			for _, rel := range aView.With {
				if rel == nil || rel.Of == nil {
					continue
				}
				bindViewTemplateParameters(&rel.Of.View, params)
			}
			return
		}
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
	if resourceUsesVelty(resource) {
		if rebuilt := ensureSchemaTypeVeltyAliases(param.Schema.Type()); rebuilt != nil {
			param.Schema.SetType(rebuilt)
			if aView.Schema != nil && schemaNeedsVeltyAliases(aView.Schema.Type()) {
				aView.Schema.SetType(rebuilt)
			}
		}
	}
	if param.Schema.Cardinality == "" {
		if required {
			param.Schema.Cardinality = state.One
		} else if aView.Schema.Cardinality != "" {
			param.Schema.Cardinality = aView.Schema.Cardinality
		}
	}
}

func resourceUsesVelty(resource *view.Resource) bool {
	if resource == nil {
		return false
	}
	for _, aView := range resource.Views {
		if aView != nil && aView.Mode == view.ModeExec {
			return true
		}
	}
	return false
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
	if schema.Cardinality == state.One && schema.Type() != nil {
		if normalized := collapseSchemaTypeToOne(schema.Type()); normalized != nil {
			schema.SetType(normalized)
			if strings.TrimSpace(schema.DataType) == "" || strings.HasPrefix(strings.TrimSpace(schema.DataType), "[]") {
				schema.DataType = normalized.String()
			}
		}
	}
	param.Schema = &schema
}

func collapseSchemaTypeToOne(rType reflect.Type) reflect.Type {
	if rType == nil {
		return nil
	}
	switch rType.Kind() {
	case reflect.Slice:
		return rType.Elem()
	case reflect.Ptr:
		elem := collapseSchemaTypeToOne(rType.Elem())
		if elem == nil {
			return nil
		}
		if elem.Kind() == reflect.Slice {
			return elem
		}
		return reflect.PtrTo(elem)
	default:
		return rType
	}
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
	if schema.Type() == nil && root.Schema.Type() != nil {
		schema.SetType(root.Schema.Type())
	}
	if schema.Cardinality == state.One && schema.Type() != nil {
		if normalized := collapseSchemaTypeToOne(schema.Type()); normalized != nil {
			schema.SetType(normalized)
			if strings.TrimSpace(schema.DataType) == "" || strings.HasPrefix(strings.TrimSpace(schema.DataType), "[]") {
				schema.DataType = normalized.String()
			}
		}
	}
	param.Schema = &schema
}

func ensureMaterializedOutputSchema(param *state.Parameter, root *view.View, source *shape.Source, ctx *typectx.Context) {
	if param == nil || param.In == nil {
		return
	}
	if param.In.Kind != state.KindOutput {
		return
	}
	switch strings.ToLower(strings.TrimSpace(param.In.Name)) {
	case "status":
		if param.Schema != nil && (param.Schema.Type() != nil || strings.TrimSpace(param.Schema.DataType) != "") {
			return
		}
		param.Schema = state.NewSchema(reflect.TypeOf(response.Status{}))
	case "summary":
		if (param.Schema == nil || (param.Schema.Type() == nil && strings.TrimSpace(param.Schema.DataType) == "" || strings.TrimSpace(param.Schema.DataType) == "?")) && root != nil && root.Template != nil && root.Template.Summary != nil && root.Template.Summary.Schema != nil {
			param.Schema = root.Template.Summary.Schema.Clone()
		}
		if (param.Schema == nil || (param.Schema.Type() == nil && (strings.TrimSpace(param.Schema.DataType) == "" || strings.TrimSpace(param.Schema.DataType) == "?"))) && strings.TrimSpace(param.Name) != "" {
			if summaryType := resolveSummarySchemaType(source, ctx, param.Name); summaryType != nil {
				param.Schema = materializedSummarySchema(summaryType, param.Name, ctx)
			}
		}
	}
}
