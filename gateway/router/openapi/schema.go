package openapi

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view/state"
	"github.com/viant/xdatly/docs"
	"github.com/viant/xreflect"
	"reflect"
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
		doc        docs.Service
	}

	Schema struct {
		pkg         string
		path        string
		fieldName   string
		name        string
		description string
		rType       reflect.Type
		tag         Tag
	}

	SchemaContainer struct {
		mux              sync.Mutex
		schemas          []*openapi3.Schema
		index            map[string]int
		generatedSchemas map[string]*openapi3.Schema
	}
)

func (s *Schema) ReplaceType(rType reflect.Type) *Schema {
	result := *s
	result.rType = rType
	return &result
}

func (s *Schema) Field(field reflect.StructField, tag Tag) (*Schema, error) {
	result := *s
	result.path = result.path + "." + field.Name
	result.rType = field.Type
	result.tag = tag
	result.fieldName = field.Name

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
	result, err := c.TypedSchema(ctx, inputType, "RequestBody")
	if err != nil {
		return nil, err
	}

	result.tag.IsNullable = !c.isRequired(c.component.Input)
	return result, nil
}

func (c *ComponentSchema) ResponseBody(ctx context.Context) (*Schema, error) {
	schema, err := c.TypedSchema(ctx, c.component.Output.Type, "ResponseBody")
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func (c *ComponentSchema) TypedSchema(ctx context.Context, stateType state.Type, defaultTypeName string) (*Schema, error) {
	rType := stateType.Schema.Type()
	typeName := c.TypeName(rType, defaultTypeName)
	path := stateType.Package + "." + typeName

	description, err := c.Description(ctx, path, SuccessSchemaDescription)
	if err != nil {
		return nil, err
	}

	return &Schema{
		pkg:         stateType.Package,
		path:        path,
		name:        typeName,
		fieldName:   typeName,
		description: description,
		rType:       rType,
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

func (c *ComponentSchema) TypeName(schemaType reflect.Type, defaultValue string) string {
	types := c.component.TypeRegistry()
	aType := types.Info(schemaType)
	if aType != nil {
		return aType.Name
	}

	return defaultValue
}

func (c *ComponentSchema) ReflectSchema(name string, rType reflect.Type, description string) *Schema {
	return c.SchemaWithTag(name, rType, description, Tag{})
}

func (c *ComponentSchema) SchemaWithTag(name string, rType reflect.Type, description string, tag Tag) *Schema {
	stringified := rType.String()
	return &Schema{
		path:        stringified,
		fieldName:   name,
		name:        stringified,
		description: description,
		rType:       rType,
		tag:         tag,
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

func (c *SchemaContainer) addToSchema(ctx context.Context, component *ComponentSchema, dst *openapi3.Schema, schema *Schema) error {
	rType := schema.rType
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	switch rType.Kind() {
	case reflect.Slice, reflect.Array:
		var err error
		dst.Items, err = c.createSchema(ctx, component, schema.ReplaceType(rType.Elem()))
		if err != nil {
			return err
		}
		dst.Type = arrayOutput
	case reflect.Struct:
		if rType == xreflect.TimeType {
			dst.Type = stringOutput
			dateFormat := schema.tag.Format
			if dateFormat == "" {
				dateFormat = time.RFC3339
			}
			dst.Format = dateFormat
			break
		}

		dst.Properties = openapi3.Schemas{}
		dst.Type = objectOutput
		numField := rType.NumField()
		for i := 0; i < numField; i++ {
			aField := rType.Field(i)
			if aField.PkgPath != "" {
				continue
			}

			aTag, err := ParseTag(aField, aField.Tag)
			if err != nil {
				return err
			}

			if aTag.Ignore {
				continue
			}

			if aTag.Inlined {
				dst.AdditionalPropertiesAllowed = setter.BoolPtr(true)
				continue
			}

			fieldSchema, err := schema.Field(aField, aTag)
			if err != nil {
				return err
			}

			if _, ok := component.component.Output.Excluded()[fieldSchema.path]; ok {
				continue
			}

			if aField.Anonymous {
				if err := c.addToSchema(ctx, component, dst, schema); err != nil {
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
			dst.AnyOf = openapi3.SchemaList{
				{
					Type: stringOutput,
				},
				{
					Type: objectOutput,
				},
				{
					Type: arrayOutput,
				},
				{
					Type: numberOutput,
				},
				{
					Type: booleanOutput,
				},
			}
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

func (c *ComponentSchema) GetOrGenerateSchema(ctx context.Context, schema *Schema) (*openapi3.Schema, error) {
	return c.schemas.CreateSchema(ctx, c, schema)
}

func (c *SchemaContainer) CreateSchema(ctx context.Context, componentSchema *ComponentSchema, fieldSchema *Schema) (*openapi3.Schema, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.createSchema(ctx, componentSchema, fieldSchema)
}

func (c *SchemaContainer) createSchema(ctx context.Context, componentSchema *ComponentSchema, fieldSchema *Schema) (*openapi3.Schema, error) {
	apiType, format, err := c.toOpenApiType(fieldSchema.rType)
	if err == nil {
		return &openapi3.Schema{
			Type:   apiType,
			Format: format,
		}, nil
	}

	if currentSchema := c.generatedSchemas[fieldSchema.name]; currentSchema != nil {
		*currentSchema = *c.SchemaRef(fieldSchema.name)
		c.generatedSchemas[fieldSchema.name] = nil //do not delete, just mark it was replaced with schema reference
	}

	schemaName := fieldSchema.name

	_, ok := c.generatedSchemas[schemaName]
	if !ok {
		generatedSchema, err := componentSchema.GenerateSchema(ctx, fieldSchema)
		if err != nil {
			return nil, err
		}

		c.index[schemaName] = len(c.schemas)
		c.schemas = append(c.schemas, generatedSchema)
		c.generatedSchemas[schemaName] = generatedSchema
		return generatedSchema, err
	}

	return c.SchemaRef(schemaName), nil
}

func (c *SchemaContainer) SchemaRef(schemaName string) *openapi3.Schema {
	return &openapi3.Schema{Ref: "#/components/schemas/" + schemaName}
}

func (c *SchemaContainer) toOpenApiType(rType reflect.Type) (string, string, error) {
	switch rType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
		return integerOutput, int64Format, nil
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return integerOutput, int32Format, nil
	case reflect.Float64, reflect.Float32:
		return numberOutput, doubleFormat, nil
	case reflect.Bool:
		return booleanOutput, empty, nil
	case reflect.String:
		return stringOutput, empty, nil
	}

	return empty, empty, fmt.Errorf("unsupported openapi3 type %v", rType.String())
}
