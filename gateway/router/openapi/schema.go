package openapi

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/tagly/format/text"
	ftime "github.com/viant/tagly/format/time"
	"github.com/viant/xdatly/docs"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	SuccessSchemaDescription = "Success object schema"
)

type (
	ComponentSchema struct {
		component  *repository.Component
		components *repository.Service
		schemas    *SchemaContainer
		docs       *state.Docs
		doc        docs.Service
	}

	Schema struct {
		docs        *state.Docs
		pkg         string
		path        string
		fieldName   string
		name        string
		description string
		example     string
		rType       reflect.Type
		tag         Tag
		ioConfig    *config.IOConfig
		isInput     bool
	}

	SchemaContainer struct {
		mux              sync.Mutex
		schemas          []*openapi3.Schema
		index            map[string]int
		generatedSchemas map[string]*openapi3.Schema
	}
)

func (s *Schema) SliceItem(rType reflect.Type) *Schema {
	result := *s
	elem := rType.Elem()
	if elem.Name() != "" {
		result.tag.TypeName = elem.Name()
	} else {
		result.tag.TypeName = result.tag.TypeName + "Item"
	}
	result.rType = elem
	return &result
}

func (s *Schema) Field(field reflect.StructField, tag *Tag) (*Schema, error) {

	result := *s
	result.path = result.path + "." + field.Name
	result.rType = field.Type
	if tag != nil {
		result.tag = *tag
	}
	result.fieldName = s.ioConfig.FormatName(field.Name)
	if tag.JSONName != "" {
		result.fieldName = tag.JSONName
	}
	result.name = tag._tag.FormatName()
	if result.name == "" {
		result.name = s.ioConfig.CaseFormat.Format(result.fieldName, text.CaseFormatUpperCamel)
	}

	result.description = field.Tag.Get(tags.DescriptionTag)
	result.example = field.Tag.Get(tags.ExampleTag)

	return &result, nil
}

func NewContainer() *SchemaContainer {
	return &SchemaContainer{
		index:            map[string]int{},
		generatedSchemas: map[string]*openapi3.Schema{},
	}
}

func NewComponentSchema(components *repository.Service, component *repository.Component, container *SchemaContainer) *ComponentSchema {
	if container == nil {
		container = NewContainer()
	}

	doc, _ := component.Doc()
	return &ComponentSchema{
		components: components,
		component:  component,
		schemas:    container,
		doc:        doc,
	}
}

func (c *ComponentSchema) RequestBody(ctx context.Context) (*Schema, error) {
	inputType := c.component.Input.Type

	name := inputType.SimpleTypeName()
	if name == "" {
		name = "Input"
	}

	result, err := c.TypedSchema(ctx, inputType, name, c.component.IOConfig(), true)
	if err != nil {
		return nil, err
	}

	result.tag.IsNullable = !c.isRequired(c.component.Input)
	return result, nil
}

func (c *ComponentSchema) ResponseBody(ctx context.Context) (*Schema, error) {

	name := c.component.Output.Type.SimpleTypeName()
	if name == "" {
		name = "Output"
	}
	schema, err := c.TypedSchema(ctx, c.component.Output.Type, name, c.component.IOConfig(), false)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func (c *ComponentSchema) TypedSchema(ctx context.Context, stateType state.Type, defaultTypeName string, ioConfig *config.IOConfig, isInput bool) (*Schema, error) {
	rType := stateType.Schema.Type()
	typeName := c.TypeName(rType, defaultTypeName)
	path := stateType.Package + "." + typeName

	description, err := c.Description(ctx, path, SuccessSchemaDescription)
	if err != nil {
		return nil, err
	}

	return &Schema{
		tag:         Tag{TypeName: defaultTypeName},
		pkg:         stateType.Package,
		path:        path,
		name:        typeName,
		fieldName:   typeName,
		description: description,
		rType:       rType,
		ioConfig:    ioConfig,
		isInput:     isInput,
		docs:        c.component.Docs(),
	}, nil
}

func (c *ComponentSchema) isRequired(input contract.Input) bool {
	isRequired := false
	for _, parameter := range input.Body.Parameters {
		if parameter.IsRequired() {
			isRequired = true
			break
		}
	}
	return isRequired
}

func (c *ComponentSchema) Description(ctx context.Context, path string, defaultDescription string) (string, error) {
	if c.doc != nil {
		lookupDesc, ok, err := c.doc.Lookup(ctx, path)
		if err != nil {
			return "", err
		}

		if ok {
			return lookupDesc, nil
		}
	}
	return defaultDescription, nil
}

func (c *ComponentSchema) Example(ctx context.Context, path string, defaultDescription string) (string, error) {
	if c.doc != nil {
		lookupDesc, ok, err := c.doc.Lookup(ctx, path+"$example")
		if err != nil {
			return "", err
		}

		if ok {
			return lookupDesc, nil
		}
	}
	return defaultDescription, nil
}

func (c *ComponentSchema) TypeName(schemaType reflect.Type, defaultValue string) string {
	types := c.component.TypeRegistry()
	aType := types.Info(schemaType)
	if aType != nil {
		return aType.Name
	}

	return defaultValue
}

func (c *ComponentSchema) ReflectSchema(name string, rType reflect.Type, description string, ioConfig *config.IOConfig) *Schema {
	return c.SchemaWithTag(name, rType, description, ioConfig, Tag{})
}

func (c *ComponentSchema) SchemaWithTag(fieldName string, rType reflect.Type, description string, ioConfig *config.IOConfig, tag Tag) *Schema {

	if parameter := tag.Parameter; parameter != nil {
		if parameter.DataType != "" {
			typeLookup := c.component.View.Resource().LookupType()
			if lType, _ := typeLookup(parameter.DataType); lType != nil {
				rType = lType
			}
		}
	}
	typeName := rType.String()
	if ioConfig.CaseFormat.IsDefined() {
		fieldName = ioConfig.FormatName(typeName)
	}
	return &Schema{
		path:        typeName,
		fieldName:   fieldName,
		name:        typeName,
		description: description,
		rType:       rType,
		tag:         tag,
		ioConfig:    ioConfig,
		docs:        c.component.Docs(),
	}
}
func (c *ComponentSchema) GenerateSchema(ctx context.Context, schema *Schema) (*openapi3.Schema, error) {
	description, err := c.Description(ctx, schema.path, "")
	if err != nil {
		return nil, err
	}

	result := &openapi3.Schema{
		Description:  description,
		Nullable:     schema.tag.IsNullable,
		Min:          schema.tag.Min,
		Max:          schema.tag.Max,
		ExclusiveMax: schema.tag.ExclusiveMax,
		ExclusiveMin: schema.tag.ExclusiveMin,
		MaxLength:    schema.tag.MaxLength,
		MinLength:    schema.tag.MinLength,
		WriteOnly:    schema.tag.WriteOnly,
		ReadOnly:     schema.tag.ReadOnly,
		MaxItems:     schema.tag.MaxItems,
		Default:      schema.tag.Default,
		Example:      schema.tag.Example,
	}

	if err := c.schemas.addToSchema(ctx, c, result, schema); err != nil {
		return nil, err
	}

	return result, nil
}

// TODO refactor
func (c *SchemaContainer) addToSchema(ctx context.Context, component *ComponentSchema, dst *openapi3.Schema, schema *Schema) error {
	rType := schema.rType
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	if schema.tag.Example != "" {
		dst.Example = schema.tag.Example
	}

	rootTable := ""

	if component.component.View.Mode == view.ModeQuery {
		rootTable = component.component.View.Table
	}
	switch rType.Kind() {
	case reflect.Slice, reflect.Array:
		var err error
		dst.Items, err = c.createSchema(ctx, component, schema.SliceItem(rType))
		if err != nil {
			return err
		}
		dst.Type = arrayOutput
	case reflect.Struct:
		if rType == xreflect.TimeType {
			dst.Type = stringOutput
			timeLayout := schema.tag._tag.TimeLayout
			if timeLayout == "" {
				timeLayout = time.RFC3339
			}

			var dateFormat string
			if containsAny(timeLayout, "15", "04", "05") {
				dateFormat = "date-time"
			} else {
				dateFormat = "date"
			}

			dst.Format = dateFormat
			if dst.Example == nil {
				dst.Example = time.Now().Format(timeLayout)
			}

			dst.Pattern = ftime.TimeLayoutToDateFormat(timeLayout)
			break
		}

		dst.Properties = openapi3.Schemas{}
		dst.Type = objectOutput
		numField := rType.NumField()
		table := schema.tag.Table
		for i := 0; i < numField; i++ {
			aField := rType.Field(i)
			if aField.PkgPath != "" {
				continue
			}
			aTag, err := ParseTag(aField, aField.Tag, schema.isInput, rootTable)
			if err != nil {
				return err
			}
			if aTag.Table == "" {
				aTag.Table = table
			}
			if aTag.Ignore {
				continue
			}

			if aTag.Column != "" && table == "" {
				table = rootTable
				aTag.Table = rootTable
			}
			if table != "" && aTag.Column == "" {
				aTag.Column = text.DetectCaseFormat(aField.Name).To(text.CaseFormatUpperUnderscore).Format(aField.Name)
			}

			if aTag.Inlined {
				dst.AdditionalPropertiesAllowed = setter.BoolPtr(true)
				continue
			}
			fieldSchema, err := schema.Field(aField, aTag)
			if err != nil {
				return err
			}

			if component.component.Output.IsExcluded(fieldSchema.path) {
				continue
			}

			docs := component.component.Docs()
			updatedDocumentation(aTag, docs, fieldSchema)

			if aField.Anonymous {
				if err := c.addToSchema(ctx, component, dst, fieldSchema); err != nil {
					return err
				}
				continue
			}

			if len(dst.Properties) == 0 {
				dst.Properties = make(openapi3.Schemas)
			}
			dst.Properties[fieldSchema.fieldName], err = c.createSchema(ctx, component, fieldSchema)
			if err != nil {
				return err
			}

			if !aTag.IsNullable {
				dst.Required = append(dst.Required, fieldSchema.fieldName)
			}
		}
	default:
		if rType.Kind() == reflect.Interface {
			dst.Type = objectOutput
			break
		}

		if rType.Kind() == reflect.Map {
			dst.Type = objectOutput
			keyType := rType.Key()
			valueType := rType.Elem()
			valueTypeName := valueType.Name()
			vType, format, err := c.toOpenApiType(valueType)
			valueSchema := &openapi3.Schema{
				Type:   vType,
				Format: format,
			}
			if err != nil {
				switch valueType.Kind() {
				case reflect.Struct:
				case reflect.Slice:

					if vType, format, err = c.toOpenApiType(valueType.Elem()); err != nil {
						return err
					}
					valueTypeName += strings.Title(valueType.Elem().Name()) + "s"
					valueSchema.Type = arrayOutput
					valueSchema.Items = &openapi3.Schema{
						Type:   vType,
						Format: format,
					}
				default:
					return err
				}
			}
			dst.Properties = openapi3.Schemas{}
			mapType := strings.Title(keyType.Name()) + valueTypeName + "Map"
			dst.Properties[mapType] = valueSchema
			break
		}

		var err error
		dst.Type, dst.Format, err = c.toOpenApiType(rType)
		if err != nil {
			return err
		}
	}

	return nil
}

func updatedDocumentation(aTag *Tag, docs *state.Docs, fieldSchema *Schema) {
	if docs == nil {
		return
	}
	if aTag.Column != "" && len(docs.Columns) > 0 {
		columns := docs.Columns
		if aTag.Description == "" {
			aTag.Description, _ = columns.ColumnDescription(aTag.Table, aTag.Column)
		}
		if aTag.Description == "" {
			aTag.Description, _ = columns.ColumnDescription("", aTag.Column)
		}
		if aTag.Example == "" {
			aTag.Example, _ = columns.ColumnExample(aTag.Table, aTag.Column)
		}
	}
	if aTag.Description == "" && len(docs.Paths) > 0 {
		if desc, ok := docs.Paths.ByName(fieldSchema.path); ok {
			aTag.Description = desc
		} else if desc, ok := docs.Paths.ByName(fieldSchema.name); ok {
			aTag.Description = desc
			fieldSchema.description = desc
		}
	}
	if aTag.Description != "" {
		fieldSchema.description = aTag.Description
	}
	if aTag.Example != "" {
		fieldSchema.example = aTag.Example
	}

}

func containsAny(format string, values ...string) bool {
	for _, value := range values {
		if strings.Contains(format, value) {
			return true
		}
	}

	return false
}

func (c *ComponentSchema) GetOrGenerateSchema(ctx context.Context, schema *Schema) (*openapi3.Schema, error) {
	return c.schemas.CreateSchema(ctx, c, schema)
}

func (c *SchemaContainer) CreateSchema(ctx context.Context, componentSchema *ComponentSchema, fieldSchema *Schema) (*openapi3.Schema, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.createSchema(ctx, componentSchema, fieldSchema)
}

func (c *SchemaContainer) createSchema(ctx context.Context, componentSchema *ComponentSchema, fieldSchema *Schema) (*openapi3.Schema, error) {
	description, err := componentSchema.Description(ctx, fieldSchema.path, fieldSchema.description)
	if err != nil {
		return nil, err
	}
	example, err := componentSchema.Example(ctx, fieldSchema.path, fieldSchema.example)
	if err != nil {
		return nil, err
	}

	if fieldSchema.tag.TypeName != "" {
		_, ok := c.generatedSchemas[fieldSchema.tag.TypeName]
		if ok {
			return c.SchemaRef(fieldSchema.tag.TypeName, description), nil
		}
	}

	apiType, format, ok := c.asOpenApiType(fieldSchema.rType)
	if ok {
		return &openapi3.Schema{
			Type:        apiType,
			Format:      format,
			Description: description,
			Example:     example,
		}, nil
	}

	schema, err := componentSchema.GenerateSchema(ctx, fieldSchema)
	if err != nil {
		return nil, err
	}

	if fieldSchema.tag.TypeName != "" {
		c.generatedSchemas[fieldSchema.tag.TypeName] = schema
		c.schemas = append(c.schemas, schema)
		schema = c.SchemaRef(fieldSchema.tag.TypeName, description)
	}

	return schema, err
}

func (c *SchemaContainer) SchemaRef(schemaName string, description string) *openapi3.Schema {
	return &openapi3.Schema{
		Ref:         "#/components/schemas/" + schemaName,
		Description: description,
	}
}

func (c *SchemaContainer) toOpenApiType(rType reflect.Type) (string, string, error) {
	apiType, format, ok := c.asOpenApiType(rType)
	if !ok {
		return empty, empty, fmt.Errorf("unsupported openapi3 type %v", rType.String())
	}
	return apiType, format, nil
}

func (c *SchemaContainer) asOpenApiType(rType reflect.Type) (string, string, bool) {
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	switch rType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
		return integerOutput, int64Format, true
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return integerOutput, int32Format, true
	case reflect.Float64, reflect.Float32:
		return numberOutput, doubleFormat, true
	case reflect.Bool:
		return booleanOutput, empty, true
	case reflect.String:
		return stringOutput, empty, true
	}

	return empty, empty, false
}
