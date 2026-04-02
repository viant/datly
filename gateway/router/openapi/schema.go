package openapi

import (
	"context"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/docs"
	"github.com/viant/xreflect"
	"reflect"
	"sync"
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
		visitingTypes    map[string]int
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
		visitingTypes:    map[string]int{},
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
		if tag.IsInput && parameter.DataType != "" {
			var typeLookup xreflect.LookupType
			if c.component != nil && c.component.View != nil && c.component.View.Resource() != nil {
				typeLookup = c.component.View.Resource().LookupType()
			}
			if lType, _ := types.LookupType(typeLookup, parameter.DataType); lType != nil {
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
