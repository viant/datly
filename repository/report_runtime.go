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

func isReportEligible(component *Component) bool {
	if component == nil || component.Report == nil || !component.Report.Enabled {
		return false
	}
	if component.View == nil || !component.View.Groupable {
		return false
	}
	return strings.EqualFold(component.Method, http.MethodGet)
}

func (s *Service) buildReportComponent(original *Component, routePath *path.Path) (*Component, *path.Path, error) {
	return buildReportArtifacts(context.Background(), s.registry.Dispatcher(), original, routePath)
}

func BuildReportComponent(dispatcher contract.Dispatcher, original *Component) (*Component, error) {
	component, _, err := buildReportArtifacts(context.Background(), dispatcher, original, nil)
	return component, err
}

func buildReportArtifacts(ctx context.Context, dispatcher contract.Dispatcher, original *Component, routePath *path.Path) (*Component, *path.Path, error) {
	config := original.Report.normalize()
	metadata, err := buildReportMetadata(original, config)
	if err != nil {
		return nil, nil, err
	}
	inputType, err := buildReportInputType(original, metadata, config)
	if err != nil {
		return nil, nil, err
	}
	reportURI := strings.TrimSuffix(original.URI, "/") + "/report"
	ret := *original
	ret.Path = contract.Path{Method: http.MethodPost, URI: reportURI}
	ret.Handler = rephandler.NewHandler(&reportHandler{
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
		pathCopy.MCPTool = config.mcpToolEnabled()
		pathCopy.MCPResource = false
		pathCopy.MCPTemplateResource = false
		pathCopy.Report = routePath.Report
		if pathCopy.Name != "" {
			pathCopy.Name += " Report"
		}
		if pathCopy.Description != "" {
			pathCopy.Description += " report"
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
		Name:        original.Name + "#report",
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
		URI:    strings.TrimSuffix(routePath.URI, "/") + "/report",
	}
	pathCopy.MCPTool = reportPathMCPToolEnabled(routePath.Report)
	pathCopy.MCPResource = false
	pathCopy.MCPTemplateResource = false
	if pathCopy.Name != "" {
		pathCopy.Name += " Report"
	}
	if pathCopy.Description != "" {
		pathCopy.Description += " report"
	}
	return &pathCopy
}

func reportPathMCPToolEnabled(report *path.Report) bool {
	if report == nil || report.MCPTool == nil {
		return true
	}
	return *report.MCPTool
}

func buildReportMetadata(component *Component, report *Report) (*ReportMetadata, error) {
	report = report.normalize()
	viewRef := component.View
	if viewRef == nil {
		return nil, fmt.Errorf("report component view was empty")
	}
	result := &ReportMetadata{
		InputName:     report.inputTypeName(component.Name, component.Input.Type.Name, viewRef.Name),
		BodyFieldName: "",
		DimensionsKey: report.Dimensions,
		MeasuresKey:   report.Measures,
		FiltersKey:    report.Filters,
		OrderBy:       report.OrderBy,
		Limit:         report.Limit,
		Offset:        report.Offset,
	}
	for _, column := range viewRef.Columns {
		if column == nil || column.FieldName() == "" {
			continue
		}
		fieldName := exportedReportFieldName(column.FieldName())
		field := &ReportField{Name: column.FieldName(), FieldName: fieldName, Description: column.Name}
		switch {
		case column.Groupable:
			field.Section = report.Dimensions
			result.Dimensions = append(result.Dimensions, field)
		case column.Aggregate || (viewRef.Groupable && !column.Groupable):
			field.Section = report.Measures
			result.Measures = append(result.Measures, field)
		}
	}
	for _, parameter := range component.Input.Type.Parameters {
		if parameter == nil || len(parameter.Predicates) == 0 || parameter.In == nil {
			continue
		}
		if isSelectorParameter(parameter, viewRef) {
			continue
		}
		result.Filters = append(result.Filters, &ReportFilter{
			Name:        parameter.Name,
			FieldName:   exportedReportFieldName(parameter.Name),
			Section:     report.Filters,
			Description: parameter.Description,
			Parameter:   parameter,
		})
	}
	if err := result.validateSelection(); err != nil {
		return nil, err
	}
	return result, nil
}

func buildReportInputType(component *Component, metadata *ReportMetadata, report *Report) (*state.Type, error) {
	if report != nil && report.Input != "" {
		schema := state.NewSchema(nil, state.WithSchemaPackage(""), state.WithModulePath(""))
		schema.Name = strings.TrimSpace(report.Input)
		inputType, err := state.NewType(state.WithSchema(schema), state.WithResource(component.View.Resource()))
		if err != nil {
			return nil, err
		}
		if err := inputType.Init(); err != nil {
			return nil, err
		}
		return inputType, validateExplicitReportInput(inputType, metadata)
	}
	bodyType := reflect.PtrTo(synthesizeReportBodyType(metadata))
	bodySchema := state.NewSchema(bodyType)
	bodySchema.Name = metadata.InputName
	bodyParam := state.NewParameter("Report", state.NewBodyLocation(""), state.WithParameterSchema(bodySchema))
	bodyParam.Tag = `anonymous:"true"`
	bodyParam.SetTypeNameTag()
	// Synthetic report input must not initialize against the original component resource.
	// Using the shared resource resolves linked named types and mutates the original
	// component generation state, which breaks repeated code generation.
	inputResource := newReportInputResource(component.View.Resource())
	inputType, err := state.NewType(
		state.WithParameters(state.Parameters{bodyParam}),
		state.WithBodyType(true),
		state.WithSchema(state.NewSchema(bodyType)),
		state.WithResource(inputResource),
	)
	if err != nil {
		return nil, err
	}
	if err := inputType.Init(); err != nil {
		return nil, err
	}
	inputType.Name = metadata.InputName
	return inputType, nil
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
		if schemaType := filter.schemaType(); schemaType != nil {
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
