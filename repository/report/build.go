package report

import (
	"context"
	"embed"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

type Component struct {
	Name       string
	InputName  string
	Parameters state.Parameters
	View       *view.View
	Resource   state.Resource
	Report     *Config
}

func AssembleMetadata(component *Component, cfg *Config) (*Metadata, error) {
	if component == nil {
		return nil, fmt.Errorf("report component was empty")
	}
	cfg = normalizeConfig(component, cfg)
	viewRef := component.View
	if viewRef == nil {
		return nil, fmt.Errorf("report component view was empty")
	}
	result := &Metadata{
		InputName:     cfg.InputTypeName(component.Name, component.InputName, viewRef.Name),
		BodyFieldName: "",
		DimensionsKey: cfg.Dimensions,
		MeasuresKey:   cfg.Measures,
		FiltersKey:    cfg.Filters,
		OrderBy:       cfg.OrderBy,
		Limit:         cfg.Limit,
		Offset:        cfg.Offset,
	}
	for _, column := range viewRef.Columns {
		if column == nil || column.FieldName() == "" {
			continue
		}
		fieldName := exportedFieldName(column.FieldName())
		field := &Field{Name: column.FieldName(), FieldName: fieldName, Description: column.Name}
		switch {
		case column.Groupable:
			field.Section = cfg.Dimensions
			result.Dimensions = append(result.Dimensions, field)
		case column.Aggregate || (viewRef.Groupable && !column.Groupable):
			field.Section = cfg.Measures
			result.Measures = append(result.Measures, field)
		}
	}
	for _, parameter := range component.Parameters {
		if parameter == nil || len(parameter.Predicates) == 0 || parameter.In == nil {
			continue
		}
		if isSelectorParameter(parameter, viewRef) {
			continue
		}
		result.Filters = append(result.Filters, &Filter{
			Name:        parameter.Name,
			FieldName:   exportedFieldName(parameter.Name),
			Section:     cfg.Filters,
			Description: parameter.Description,
			Parameter:   parameter,
		})
	}
	if err := result.ValidateSelection(); err != nil {
		return nil, err
	}
	return result, nil
}

func BuildBodyType(metadata *Metadata) reflect.Type {
	var fields []reflect.StructField
	fields = append(fields, reflect.StructField{
		Name: metadata.DimensionsKey,
		Type: sectionStructType(metadata.Dimensions),
		Tag:  buildTag(lowerCamel(metadata.DimensionsKey), "Selected grouping dimensions"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.MeasuresKey,
		Type: sectionStructType(metadata.Measures),
		Tag:  buildTag(lowerCamel(metadata.MeasuresKey), "Selected aggregate measures"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.FiltersKey,
		Type: filterStructType(metadata.Filters),
		Tag:  buildTag(lowerCamel(metadata.FiltersKey), "Report filters derived from original predicate parameters"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.OrderBy,
		Type: reflect.TypeOf([]string{}),
		Tag:  buildTag(lowerCamel(metadata.OrderBy), "Ordering expressions applied to the grouped result"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.Limit,
		Type: reflect.TypeOf((*int)(nil)),
		Tag:  buildTag(lowerCamel(metadata.Limit), "Maximum number of grouped rows to return"),
	})
	fields = append(fields, reflect.StructField{
		Name: metadata.Offset,
		Type: reflect.TypeOf((*int)(nil)),
		Tag:  buildTag(lowerCamel(metadata.Offset), "Row offset applied to the grouped result"),
	})
	return reflect.StructOf(fields)
}

func BuildInputType(component *Component, metadata *Metadata, cfg *Config) (*state.Type, error) {
	if component == nil {
		return nil, fmt.Errorf("report component was empty")
	}
	if metadata == nil {
		return nil, fmt.Errorf("report metadata was empty")
	}
	cfg = normalizeConfig(component, cfg)
	if cfg.Input != "" {
		schema := state.NewSchema(nil, state.WithSchemaPackage(""), state.WithModulePath(""))
		schema.Name = strings.TrimSpace(cfg.Input)
		inputType, err := state.NewType(state.WithSchema(schema), state.WithResource(component.resource()))
		if err != nil {
			return nil, err
		}
		if err := inputType.Init(); err != nil {
			return nil, err
		}
		return inputType, validateExplicitInput(inputType, metadata)
	}
	bodyType := reflect.PtrTo(BuildBodyType(metadata))
	bodySchema := state.NewSchema(bodyType)
	bodySchema.Name = metadata.InputName
	bodyParam := state.NewParameter("Report", state.NewBodyLocation(""), state.WithParameterSchema(bodySchema))
	bodyParam.Tag = `anonymous:"true"`
	bodyParam.SetTypeNameTag()
	inputType, err := state.NewType(
		state.WithParameters(state.Parameters{bodyParam}),
		state.WithBodyType(true),
		state.WithSchema(state.NewSchema(bodyType)),
		state.WithResource(newInputResource(component.resource())),
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

func normalizeConfig(component *Component, cfg *Config) *Config {
	if cfg != nil {
		return cfg.Normalize()
	}
	if component == nil || component.Report == nil {
		return (&Config{}).Normalize()
	}
	return component.Report.Normalize()
}

func (c *Component) resource() state.Resource {
	if c == nil {
		return nil
	}
	if c.Resource != nil {
		return c.Resource
	}
	if c.View != nil {
		return c.View.Resource()
	}
	return nil
}

func exportedFieldName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return state.SanitizeTypeName(value)
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

func validateExplicitInput(inputType *state.Type, metadata *Metadata) error {
	if inputType == nil {
		return fmt.Errorf("explicit report input type was empty")
	}
	var rType reflect.Type
	if inputType.Schema != nil {
		rType = inputType.Schema.Type()
	}
	if rType == nil && inputType.Type() != nil {
		rType = inputType.Type().Type()
	}
	if rType == nil {
		return fmt.Errorf("explicit report input state type was empty")
	}
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
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

func sectionStructType(fields []*Field) reflect.Type {
	if len(fields) == 0 {
		return reflect.TypeOf(struct{}{})
	}
	structFields := make([]reflect.StructField, 0, len(fields))
	for _, field := range fields {
		structFields = append(structFields, reflect.StructField{
			Name: field.FieldName,
			Type: reflect.TypeOf(false),
			Tag:  buildTag(lowerCamel(field.Name), field.Description),
		})
	}
	return reflect.StructOf(structFields)
}

func filterStructType(filters []*Filter) reflect.Type {
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
			Tag:  buildTag(lowerCamel(filter.Name), filter.Description),
		})
	}
	return reflect.StructOf(structFields)
}

func buildTag(jsonName, description string) reflect.StructTag {
	result := fmt.Sprintf(`json:"%s,omitempty"`, jsonName)
	if description = strings.TrimSpace(description); description != "" {
		result += " desc:" + strconv.Quote(description)
	}
	return reflect.StructTag(result)
}

func lowerCamel(value string) string {
	if value == "" {
		return ""
	}
	return text.CaseFormatUpperCamel.Format(value, text.CaseFormatLowerCamel)
}

type inputResource struct {
	base state.Resource
}

func newInputResource(base state.Resource) state.Resource {
	return &inputResource{base: base}
}

func (r *inputResource) LookupParameter(name string) (*state.Parameter, error) { return nil, nil }
func (r *inputResource) AppendParameter(parameter *state.Parameter)            {}
func (r *inputResource) ViewSchema(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *inputResource) ViewSchemaPointer(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *inputResource) LookupType() xreflect.LookupType { return nil }
func (r *inputResource) LoadText(ctx context.Context, URL string) (string, error) {
	return "", nil
}
func (r *inputResource) Codecs() *codec.Registry {
	if r.base != nil && r.base.Codecs() != nil {
		return r.base.Codecs()
	}
	return codec.New()
}
func (r *inputResource) CodecOptions() *codec.Options {
	if r.base != nil && r.base.CodecOptions() != nil {
		return r.base.CodecOptions()
	}
	return codec.NewOptions(nil)
}
func (r *inputResource) ExpandSubstitutes(value string) string {
	if r.base != nil {
		return r.base.ExpandSubstitutes(value)
	}
	return value
}
func (r *inputResource) ReverseSubstitutes(value string) string {
	if r.base != nil {
		return r.base.ReverseSubstitutes(value)
	}
	return value
}
func (r *inputResource) EmbedFS() *embed.FS                       { return nil }
func (r *inputResource) SetFSEmbedder(embedder *state.FSEmbedder) {}
