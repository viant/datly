package repository

import (
	"context"
	"embed"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/repository/contract"
	rephandler "github.com/viant/datly/repository/handler"
	"github.com/viant/datly/repository/path"
	reportmodel "github.com/viant/datly/repository/report"
	"github.com/viant/datly/service"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

func (s *Service) appendReportProvider(ctx context.Context, item *path.Item, routePath *path.Path, providers []*Provider, provider *Provider) ([]*Provider, error) {
	if routePath == nil || routePath.Report == nil || !routePath.Report.Enabled {
		return providers, nil
	}
	reportPath := buildReportPath(routePath)
	reportProvider := &Provider{
		path:    reportPath.Path,
		control: routePath.Version,
		newComponent: func(ctx context.Context, opts ...Option) (*Component, error) {
			original, err := provider.Component(ctx, opts...)
			if err != nil || original == nil {
				return nil, err
			}
			if !isReportEligible(original) {
				return nil, nil
			}
			component, _, err := buildReportArtifacts(ctx, s.registry.Dispatcher(), original, routePath)
			return component, err
		},
	}
	item.Paths = append(item.Paths, reportPath)
	providers = append(providers, reportProvider)
	return providers, nil
}

func (s *Service) appendCubeComposeProvider(ctx context.Context, item *path.Item, routePath *path.Path, providers []*Provider, provider *Provider) ([]*Provider, error) {
	if routePath == nil || routePath.Report == nil || routePath.Report.Compose == nil || !routePath.Report.Compose.Enabled {
		return providers, nil
	}
	composePath := buildCubeComposePath(routePath)
	composeProvider := &Provider{
		path:    composePath.Path,
		control: routePath.Version,
		newComponent: func(ctx context.Context, opts ...Option) (*Component, error) {
			original, err := provider.Component(ctx, opts...)
			if err != nil || original == nil {
				return nil, err
			}
			if !isCubeComposeEligible(original) {
				return nil, nil
			}
			component, _, err := buildCubeComposeArtifacts(ctx, s.registry.Dispatcher(), original, routePath)
			return component, err
		},
	}
	item.Paths = append(item.Paths, composePath)
	providers = append(providers, composeProvider)
	return providers, nil
}

func isReportEligible(component *Component) bool {
	if component == nil || component.Report == nil || !component.Report.Enabled {
		return false
	}
	if component.View == nil || !component.View.Groupable {
		return false
	}
	return strings.EqualFold(component.Method, http.MethodGet)
}

func isCubeComposeEligible(component *Component) bool {
	return isReportEligible(component) && component.Report.Compose != nil && component.Report.Compose.Enabled
}

func (s *Service) buildReportComponent(original *Component, routePath *path.Path) (*Component, *path.Path, error) {
	return buildReportArtifacts(context.Background(), s.registry.Dispatcher(), original, routePath)
}

func BuildReportComponent(dispatcher contract.Dispatcher, original *Component) (*Component, error) {
	component, _, err := buildReportArtifacts(context.Background(), dispatcher, original, nil)
	return component, err
}

func BuildCubeComposeComponent(dispatcher contract.Dispatcher, original *Component) (*Component, error) {
	component, _, err := buildCubeComposeArtifacts(context.Background(), dispatcher, original, nil)
	return component, err
}

func buildCubeComposeArtifacts(ctx context.Context, dispatcher contract.Dispatcher, original *Component, routePath *path.Path) (*Component, *path.Path, error) {
	if !isCubeComposeEligible(original) {
		return nil, nil, fmt.Errorf("cube compose requires an enabled groupable report cube")
	}
	config := original.Report.Normalize()
	metadata, err := buildReportMetadata(original, config)
	if err != nil {
		return nil, nil, err
	}
	inputType, bodyType, err := buildCubeComposeInputType(original, metadata, config.Compose)
	if err != nil {
		return nil, nil, err
	}
	outputType, err := buildCubeComposeOutputType(original)
	if err != nil {
		return nil, nil, err
	}
	composeURI := strings.TrimSuffix(original.URI, "/") + "/cube/compose"
	ret := *original
	ret.Path = contract.Path{Method: http.MethodPost, URI: composeURI}
	ret.Handler = rephandler.NewHandler(&cubeComposeHandler{
		Dispatcher: dispatcher,
		Path:       &original.Path,
		Metadata:   metadata,
		Original:   original,
		BodyType:   bodyType,
		Config:     config.Compose,
	})
	ret.Service = service.TypeExecutor
	ret.Report = config
	ret.View = buildReportWrapperView(original.View)
	ret.View.Name = original.View.Name + "#cubeCompose"
	ret.Async = nil
	ret.Input.Type = *inputType
	ret.Output.Type = *outputType
	var composePath *path.Path
	if routePath != nil {
		composePath = buildCubeComposePath(routePath)
	}
	return &ret, composePath, nil
}

func buildCubeComposeOutputType(component *Component) (*state.Type, error) {
	outputType, err := state.NewType(
		state.WithSchema(state.NewSchema(reflect.TypeOf(&cubeComposeResponse{}))),
		state.WithResource(newReportInputResource(component.View.Resource())),
	)
	if err != nil {
		return nil, err
	}
	outputType.Name = state.SanitizeTypeName(component.Name + "CubeComposeOutput")
	return outputType, nil
}

func buildReportArtifacts(ctx context.Context, dispatcher contract.Dispatcher, original *Component, routePath *path.Path) (*Component, *path.Path, error) {
	config := original.Report.Normalize()
	metadata, err := buildReportMetadata(original, config)
	if err != nil {
		return nil, nil, err
	}
	inputType, err := buildReportInputType(original, metadata, config)
	if err != nil {
		return nil, nil, err
	}
	reportURI := strings.TrimSuffix(original.URI, "/") + "/cube"
	ret := *original
	ret.Path = contract.Path{Method: http.MethodPost, URI: reportURI}
	ret.Handler = rephandler.NewHandler(&cubeHandler{
		Dispatcher: dispatcher,
		Path:       &original.Path,
		Metadata:   metadata,
		Original:   original,
		BodyType:   inputType.Schema.Type(),
	})
	ret.Service = service.TypeExecutor
	ret.Report = config
	ret.View = buildReportWrapperView(original.View)
	ret.Async = nil
	ret.Input.Type = *inputType
	var reportPath *path.Path
	if routePath != nil {
		pathCopy := *routePath
		pathCopy.Path = ret.Path
		pathCopy.View = routePath.View
		pathCopy.Internal = routePath.Internal
		pathCopy.Meta = routePath.Meta
		pathCopy.ModelContextProtocol = routePath.ModelContextProtocol
		pathCopy.MCPTool = reportMCPToolEnabled(config.MCPTool, routePath.MCPTool)
		pathCopy.MCPResource = false
		pathCopy.MCPTemplateResource = false
		pathCopy.Report = routePath.Report
		if pathCopy.Name != "" {
			pathCopy.Name += " Cube"
		}
		if pathCopy.Description != "" {
			pathCopy.Description += " cube"
		}
		reportPath = &pathCopy
	}
	return &ret, reportPath, nil
}

func buildReportWrapperView(original *view.View) *view.View {
	if original == nil {
		return nil
	}
	ret := &view.View{
		Name:        original.Name + "#cube",
		Description: original.Description,
		Module:      original.Module,
		Alias:       original.Alias,
		Mode:        view.ModeHandler,
		Connector:   original.Connector,
		CaseFormat:  original.CaseFormat,
		Groupable:   original.Groupable,
		Selector:    &view.Config{},
	}
	if original.Schema != nil {
		ret.Schema = original.Schema.Clone()
	}
	ret.SetResource(original.GetResource())
	return ret
}

func buildReportPath(routePath *path.Path) *path.Path {
	pathCopy := *routePath
	pathCopy.Path = contract.Path{
		Method: http.MethodPost,
		URI:    strings.TrimSuffix(routePath.URI, "/") + "/cube",
	}
	pathCopy.MCPTool = reportPathMCPToolEnabled(routePath.Report, routePath.MCPTool)
	pathCopy.MCPResource = false
	pathCopy.MCPTemplateResource = false
	if pathCopy.Name != "" {
		pathCopy.Name += " Cube"
	}
	if pathCopy.Description != "" {
		pathCopy.Description += " cube"
	}
	return &pathCopy
}

func buildCubeComposePath(routePath *path.Path) *path.Path {
	pathCopy := *routePath
	pathCopy.Path = contract.Path{
		Method: http.MethodPost,
		URI:    strings.TrimSuffix(routePath.URI, "/") + "/cube/compose",
	}
	pathCopy.MCPTool = routePath.Report != nil && routePath.Report.Compose != nil && cubeComposePathMCPToolEnabled(routePath.Report.Compose)
	pathCopy.MCPResource = false
	pathCopy.MCPTemplateResource = false
	if pathCopy.Name != "" {
		pathCopy.Name += " Cube Compose"
	}
	pathCopy.Description = cubeComposeToolDescription(pathCopy.Description, cubeComposeMaxCubes(routePath.Report.Compose.MaxCubes))
	return &pathCopy
}

func cubeComposeToolDescription(base string, maxCubes int) string {
	label, example := cubeComposeSQLExample(maxCubes)
	description := fmt.Sprintf("Use Cube Compose when the answer requires calculations, filtering, grouping, or ranking across independently filtered projections of this cube, or a new projection over one cube. Submit 1 to %d entries in cubes and guarded SQL in sql. Each cubes entry is prepared independently with its own typed filters and preserved bind arguments; wrapper query parameters do not become cube filters. A later entry may set inheritFrom to the one-based index of a preceding entry: omitted filters inherit and explicitly supplied filters override. align may be elapsed when the cube's time predicates support elapsed-period alignment. %s SQL must start with FROM $CubeSQL1 AS t1 and introduce every other submitted cube exactly once, in array order, using JOIN or LEFT JOIN. Join each new cube to an earlier cube with equality on exposed, same-name dimensions. Only qualified exposed dimensions and measures may be referenced. Computed projections require aliases; WHERE, GROUP BY, HAVING, ORDER BY, and a bounded LIMIT are supported. Do not submit SQL placeholders: literal values are validated and bound by Datly. Request shape example: %s. %s SQL example (replace placeholders with fields and filters exposed by this tool): %s. The SQL projection defines a request-local result shape returned as a dynamically typed Go-struct collection in data through the regular view reader. View caching, source dictionaries, and outer-view enrichment are not applied.", maxCubes, cubeComposeMappingDescription(maxCubes), cubeComposeRequestExample(maxCubes), label, example)
	if base = strings.TrimSpace(base); base != "" {
		return base + ". " + description
	}
	return description
}

func cubeComposeMappingDescription(maxCubes int) string {
	if maxCubes == 1 {
		return "cubes[0] is $CubeSQL1 AS t1."
	}
	return "cubes[0] is $CubeSQL1 AS t1, cubes[1] is $CubeSQL2 AS t2, and so on."
}

const (
	cubeComposeOneSQLExample   = "SELECT t1.<dimension>, t1.<measure> AS value_1 FROM $CubeSQL1 AS t1 ORDER BY value_1 DESC LIMIT 10"
	cubeComposeTwoSQLExample   = "SELECT t1.<dimension>, t1.<measure> AS value_1, t2.<measure> AS value_2, t1.<measure> - t2.<measure> AS difference FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.<dimension> = t2.<dimension> ORDER BY difference DESC LIMIT 10"
	cubeComposeThreeSQLExample = "SELECT t1.<dimension>, t1.<measure> AS value_1, t2.<measure> AS value_2, t3.<measure> AS value_3, t1.<measure> - t2.<measure> AS difference FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.<dimension> = t2.<dimension> LEFT JOIN $CubeSQL3 AS t3 ON t1.<dimension> = t3.<dimension> ORDER BY difference DESC LIMIT 10"
)

func cubeComposeRequestExample(maxCubes int) string {
	switch maxCubes {
	case 1:
		return `{"cubes":[{"filters":{"<filter>":"<value>"}}],"sql":"` + cubeComposeOneSQLExample + `"}`
	case 2:
		return `{"cubes":[{"filters":{"<period-filter>":"<period-1>","<scope-filter>":"<scope>"}},{"filters":{"<period-filter>":"<period-2>","<scope-filter>":"<scope>"}}],"sql":"` + cubeComposeTwoSQLExample + `"}`
	default:
		return `{"cubes":[{"filters":{"<period-filter>":"<period-1>","<scope-filter>":"<scope>"}},{"filters":{"<period-filter>":"<period-2>","<scope-filter>":"<scope>"}},{"inheritFrom":1,"filters":{"<period-filter>":"<period-3>"}}],"sql":"` + cubeComposeThreeSQLExample + `"}`
	}
}

func cubeComposeSQLExample(maxCubes int) (string, string) {
	switch maxCubes {
	case 1:
		return "One-cube", cubeComposeOneSQLExample
	case 2:
		return "Two-cube", cubeComposeTwoSQLExample
	default:
		return "Three-cube", cubeComposeThreeSQLExample
	}
}

func cubeComposePathMCPToolEnabled(compose *path.CubeCompose) bool {
	if compose == nil || compose.MCPTool == nil {
		return true
	}
	return *compose.MCPTool
}

func reportPathMCPToolEnabled(report *path.Report, parentEnabled bool) bool {
	if report == nil {
		return false
	}
	return reportMCPToolEnabled(report.MCPTool, parentEnabled)
}

func reportMCPToolEnabled(explicit *bool, parentEnabled bool) bool {
	if explicit == nil {
		return parentEnabled
	}
	return *explicit
}

func buildReportMetadata(component *Component, report *Report) (*ReportMetadata, error) {
	source := &reportmodel.Component{
		Name:       component.Name,
		InputName:  component.Input.Type.Name,
		Parameters: component.Input.Type.Parameters,
		View:       component.View,
		Resource:   component.View.Resource(),
		Report:     report,
	}
	return reportmodel.AssembleMetadata(source, report)
}

func buildReportInputType(component *Component, metadata *ReportMetadata, report *Report) (*state.Type, error) {
	source := &reportmodel.Component{
		Name:       component.Name,
		InputName:  component.Input.Type.Name,
		Parameters: component.Input.Type.Parameters,
		View:       component.View,
		Resource:   component.View.Resource(),
		Report:     report,
	}
	return reportmodel.BuildInputType(source, metadata, report)
}

func buildCubeComposeInputType(component *Component, metadata *ReportMetadata, config *CubeCompose) (*state.Type, reflect.Type, error) {
	filterType := composeFilterStructType(metadata.Filters)
	frameType := reflect.StructOf([]reflect.StructField{
		{Name: "InheritFrom", Type: reflect.TypeOf((*int)(nil)), Tag: buildReportTag("inheritFrom", "Optional one-based index of a preceding cube whose omitted filters this cube inherits")},
		{Name: "Align", Type: reflect.TypeOf(""), Tag: buildReportTag("align", "Optional frame alignment: elapsed")},
		{Name: "Filters", Type: filterType, Tag: buildReportTag("filters", "Typed filters from the source cube")},
	})
	bodyType := reflect.StructOf([]reflect.StructField{
		{Name: "Cubes", Type: reflect.SliceOf(frameType), Tag: buildReportTag("cubes", fmt.Sprintf("Ordered cube projections; requires 1 to %d entries and maps entry N to $CubeSQLN AS tN", config.MaxCubes))},
		{Name: "SQL", Type: reflect.TypeOf(""), Tag: buildReportTag("sql", cubeComposeSQLDescription(metadata, config.MaxCubes))},
	})
	bodyPtr := reflect.PtrTo(bodyType)
	bodySchema := state.NewSchema(bodyPtr)
	bodySchema.Name = state.SanitizeTypeName(component.Name + "CubeComposeInput")
	bodyParam := state.NewParameter("CubeCompose", state.NewBodyLocation(""), state.WithParameterSchema(bodySchema))
	bodyParam.Tag = `anonymous:"true"`
	bodyParam.SetTypeNameTag()
	inputType, err := state.NewType(
		state.WithParameters(state.Parameters{bodyParam}),
		state.WithBodyType(true),
		state.WithSchema(state.NewSchema(bodyPtr)),
		state.WithResource(newReportInputResource(component.View.Resource())),
	)
	if err != nil {
		return nil, nil, err
	}
	if err := inputType.Init(); err != nil {
		return nil, nil, err
	}
	inputType.Name = bodySchema.Name
	return inputType, bodyType, nil
}

func cubeComposeSQLDescription(metadata *ReportMetadata, maxCubes int) string {
	dimensions := make([]string, 0, len(metadata.Dimensions))
	for _, field := range metadata.Dimensions {
		dimensions = append(dimensions, field.Name)
	}
	measures := make([]string, 0, len(metadata.Measures))
	for _, field := range metadata.Measures {
		measures = append(measures, field.Name)
	}
	label, example := cubeComposeSQLExample(maxCubes)
	return fmt.Sprintf("Guarded SELECT over the 1 to %d ordered cubes in this request. cubes[N-1] maps exactly to $CubeSQLN AS tN. Start FROM $CubeSQL1 AS t1 and introduce every additional submitted cube exactly once, in order, with JOIN or LEFT JOIN. Each new cube must join an earlier cube using equality between exposed, same-name dimensions. Qualify every cube field with tN. You may project exposed dimensions and measures and use them in validated expressions, WHERE, GROUP BY, HAVING, and ORDER BY; every computed projection needs a safe alias. LIMIT is added automatically or must be within the configured bound. Do not use ?, named bind parameters, WITH, UNION, OFFSET, wildcard projections, arbitrary tables, or unlisted fields/functions. Datly preserves each cube projection's generated bind arguments and binds validated wrapper literals in SQL order. The result is a request-local dynamic Go-struct collection in data through the regular Datly view reader; no view cache is used, and source dictionaries and outer-view enrichment are not applied. Request shape example: %s. %s SQL example: %s. Dimensions: %s. Measures: %s", maxCubes, cubeComposeRequestExample(maxCubes), label, example, strings.Join(dimensions, ", "), strings.Join(measures, ", "))
}

func cubeComposeMaxCubes(value int) int {
	if value <= 0 {
		return reportmodel.DefaultCubeComposeMaxCubes
	}
	return value
}

func composeFilterStructType(filters []*ReportFilter) reflect.Type {
	if len(filters) == 0 {
		return reflect.TypeOf(struct{}{})
	}
	fields := make([]reflect.StructField, 0, len(filters))
	for _, filter := range filters {
		rType := reflect.TypeOf("")
		if schemaType := filter.SchemaType(); schemaType != nil {
			rType = schemaType
		}
		if rType.Kind() != reflect.Ptr {
			rType = reflect.PtrTo(rType)
		}
		fields = append(fields, reflect.StructField{
			Name: filter.FieldName,
			Type: rType,
			Tag:  buildReportTag(lowerCamel(filter.Name), filter.Description),
		})
	}
	return reflect.StructOf(fields)
}

func validateExplicitReportInput(inputType *state.Type, metadata *ReportMetadata) error {
	if inputType == nil || inputType.Type() == nil {
		return fmt.Errorf("explicit report input type was empty")
	}
	rType := inputType.Type().Type()
	if rType == nil {
		return fmt.Errorf("explicit report input state type was empty")
	}
	rType = reflectTypeOfState(rType)
	for _, fieldName := range []string{metadata.DimensionsKey, metadata.MeasuresKey, metadata.FiltersKey, metadata.OrderBy, metadata.Limit, metadata.Offset} {
		if fieldName == "" {
			continue
		}
		if _, ok := rType.FieldByName(fieldName); !ok {
			return fmt.Errorf("explicit report input %s missing field %s", rType.String(), fieldName)
		}
	}
	return nil
}

func synthesizeReportBodyType(metadata *ReportMetadata) reflect.Type {
	var fields []reflect.StructField
	fields = append(fields, reflect.StructField{
		Name: metadata.DimensionsKey,
		Type: sectionStructType(metadata.Dimensions),
		Tag:  buildReportTag(lowerCamel(metadata.DimensionsKey), "Selected grouping dimensions"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.MeasuresKey,
		Type: sectionStructType(metadata.Measures),
		Tag:  buildReportTag(lowerCamel(metadata.MeasuresKey), "Selected aggregate measures"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.FiltersKey,
		Type: filterStructType(metadata.Filters),
		Tag:  buildReportTag(lowerCamel(metadata.FiltersKey), "Report filters derived from original predicate parameters"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.OrderBy,
		Type: reflect.TypeOf([]string{}),
		Tag:  buildReportTag(lowerCamel(metadata.OrderBy), "Ordering expressions applied to the grouped result"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.Limit,
		Type: reflect.TypeOf((*int)(nil)),
		Tag:  buildReportTag(lowerCamel(metadata.Limit), "Maximum number of grouped rows to return"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.Offset,
		Type: reflect.TypeOf((*int)(nil)),
		Tag:  buildReportTag(lowerCamel(metadata.Offset), "Row offset applied to the grouped result"),
	})
	return reflect.StructOf(fields)
}

func sectionStructType(fields []*ReportField) reflect.Type {
	if len(fields) == 0 {
		return reflect.TypeOf(struct{}{})
	}
	structFields := make([]reflect.StructField, 0, len(fields))
	for _, field := range fields {
		structFields = append(structFields, reflect.StructField{
			Name: field.FieldName,
			Type: reflect.TypeOf(false),
			Tag:  buildReportTag(lowerCamel(field.Name), field.Description),
		})
	}
	return reflect.StructOf(structFields)
}

func filterStructType(filters []*ReportFilter) reflect.Type {
	if len(filters) == 0 {
		return reflect.TypeOf(struct{}{})
	}
	structFields := make([]reflect.StructField, 0, len(filters))
	for _, filter := range filters {
		rType := reflect.TypeOf("")
		if schemaType := filter.SchemaType(); schemaType != nil {
			rType = schemaType
		}
		structFields = append(structFields, reflect.StructField{
			Name: filter.FieldName,
			Type: rType,
			Tag:  buildReportTag(lowerCamel(filter.Name), filter.Description),
		})
	}
	return reflect.StructOf(structFields)
}

func buildReportTag(jsonName, description string) reflect.StructTag {
	result := fmt.Sprintf(`json:"%s,omitempty"`, jsonName)
	if description = strings.TrimSpace(description); description != "" {
		result += " desc:" + strconv.Quote(description)
	}
	return reflect.StructTag(result)
}

func isSelectorParameter(parameter *state.Parameter, aView *view.View) bool {
	if parameter == nil || parameter.In == nil {
		return false
	}
	if aView != nil && aView.Selector != nil {
		for _, selector := range []*state.Parameter{
			aView.Selector.FieldsParameter,
			aView.Selector.OrderByParameter,
			aView.Selector.LimitParameter,
			aView.Selector.OffsetParameter,
			aView.Selector.PageParameter,
		} {
			if selector != nil && selector.In != nil && selector.In.Name == parameter.In.Name {
				return true
			}
		}
	}
	name := strings.ToLower(parameter.In.Name)
	return name == "_fields" || name == "_orderby" || name == "_limit" || name == "_offset" || name == "_page" || name == "criteria"
}

func lowerCamel(value string) string {
	if value == "" {
		return ""
	}
	return text.CaseFormatUpperCamel.Format(value, text.CaseFormatLowerCamel)
}

func exportedReportFieldName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return state.SanitizeTypeName(value)
}

func reflectTypeOfState(rType reflect.Type) reflect.Type {
	if rType == nil {
		return nil
	}
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}

type reportInputResource struct {
	base state.Resource
}

func newReportInputResource(base state.Resource) state.Resource {
	return &reportInputResource{base: base}
}

func (r *reportInputResource) LookupParameter(name string) (*state.Parameter, error) { return nil, nil }
func (r *reportInputResource) AppendParameter(parameter *state.Parameter)            {}
func (r *reportInputResource) ViewSchema(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *reportInputResource) ViewSchemaPointer(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *reportInputResource) LookupType() xreflect.LookupType { return nil }
func (r *reportInputResource) LoadText(ctx context.Context, URL string) (string, error) {
	return "", nil
}
func (r *reportInputResource) Codecs() *codec.Registry {
	if r.base != nil && r.base.Codecs() != nil {
		return r.base.Codecs()
	}
	return codec.New()
}
func (r *reportInputResource) CodecOptions() *codec.Options {
	if r.base != nil && r.base.CodecOptions() != nil {
		return r.base.CodecOptions()
	}
	return codec.NewOptions(nil)
}
func (r *reportInputResource) ExpandSubstitutes(value string) string {
	if r.base != nil {
		return r.base.ExpandSubstitutes(value)
	}
	return value
}
func (r *reportInputResource) ReverseSubstitutes(value string) string {
	if r.base != nil {
		return r.base.ReverseSubstitutes(value)
	}
	return value
}
func (r *reportInputResource) EmbedFS() *embed.FS                       { return nil }
func (r *reportInputResource) SetFSEmbedder(embedder *state.FSEmbedder) {}
