package openapi

import (
	"fmt"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/tagly/choice"
	"reflect"
	"strings"
)

// enumSchema copies only the selected path so one route's domain cannot mutate
// a shared type schema used by another input or response.
func (b *schemaBuilder) enumSchema(schema *openapi3.Schema, path []string, values []any, depth int) (*openapi3.Schema, bool) {
	if schema == nil || depth > 64 {
		return schema, false
	}
	copy := *schema
	if len(path) == 0 {
		copy.Enum = append([]any(nil), values...)
		if copy.Nullable && len(copy.Enum) > 0 {
			hasNull := false
			for _, value := range copy.Enum {
				if value == nil {
					hasNull = true
					break
				}
			}
			if !hasNull {
				copy.Enum = append(copy.Enum, nil)
			}
		}
		return &copy, true
	}
	if schema.Ref != "" {
		name := strings.TrimPrefix(schema.Ref, "#/components/schemas/")
		if target := b.schemas[name]; target != nil {
			return b.enumSchema(target, path, values, depth+1)
		}
	}
	key := path[0]
	child, found := schema.Properties[key]
	if !found {
		for candidate, property := range schema.Properties {
			if !strings.EqualFold(candidate, path[0]) {
				continue
			}
			if found {
				return schema, false
			}
			key, child, found = candidate, property, true
		}
	}
	if found {
		updated, ok := b.enumSchema(child, path[1:], values, depth+1)
		if !ok {
			return schema, false
		}
		copy.Properties = openapi3.Schemas{}
		for k, v := range schema.Properties {
			copy.Properties[k] = v
		}
		copy.Properties[key] = updated
		return &copy, true
	}

	for i, child := range schema.AllOf {
		updated, ok := b.enumSchema(child, path, values, depth+1)
		if ok {
			copy.AllOf = append(openapi3.SchemaList(nil), schema.AllOf...)
			copy.AllOf[i] = updated
			return &copy, true
		}
	}
	return schema, false
}

// fieldEnum retains the native typed choice domain on the effective field.
func (b *schemaBuilder) fieldEnum(schema *openapi3.Schema, field reflect.StructField, quoted bool, inputs ...bool) (*openapi3.Schema, error) {
	input := len(inputs) > 0 && inputs[0]
	enumeration, err := choice.ParseEnum(field.Tag, field.Type)
	if err != nil {
		return nil, err
	}
	if enumeration != nil {
		if quoted {
			return nil, fmt.Errorf("enum representation conflicts with quoted JSON")
		}
		copy := *schema
		copy.Type = enumeration.WireType(input)
		copy.Format = ""
		copy.Min = nil
		copy.Max = nil
		{
			domain := enumeration.OutputDomain()
			if input {
				domain = enumeration.InputDomain()
			}
			if domain != nil {
				copy.Type = ""
				copy.Ref = ""
				copy.AllOf, copy.OneOf = nil, nil
				copy.Not, copy.Items = nil, nil
				copy.Properties, copy.Required = nil, nil
				copy.AdditionalProperties, copy.AdditionalPropertiesAllowed = nil, nil
				copy.MultipleOf = nil
				copy.ExclusiveMin, copy.ExclusiveMax = false, false
				copy.MaxLength, copy.MinLength, copy.Pattern = nil, 0, ""
				copy.MaxItems, copy.MinItems, copy.UniqueItems = nil, 0, false
				copy.MaxProps, copy.MinProps = nil, 0
				copy.Enum = nil
				copy.AnyOf = nil
				copy.Nullable = false
				if domain.OpenIntegers {
					copy.AnyOf = append(copy.AnyOf, &openapi3.Schema{Type: "integer", Min: registry.ExactSchemaIntegerBound(domain.Minimum), Max: registry.ExactSchemaIntegerBound(domain.Maximum)})
				} else if len(domain.Integers) > 0 {
					values := make([]any, len(domain.Integers))
					for i, value := range domain.Integers {
						values[i] = value
					}
					copy.AnyOf = append(copy.AnyOf, &openapi3.Schema{Type: "integer", Enum: values})
				}
				if domain.OpenStrings {
					copy.AnyOf = append(copy.AnyOf, &openapi3.Schema{Type: "string"})
				} else if len(domain.Strings) > 0 {
					values := make([]any, len(domain.Strings))
					for i, value := range domain.Strings {
						values[i] = value
					}
					copy.AnyOf = append(copy.AnyOf, &openapi3.Schema{Type: "string", Enum: values})
				}
				for _, alternative := range domain.StringAlternatives {
					if domain.OpenStrings {
						break
					}
					length := uint64(alternative.Length)
					branch := &openapi3.Schema{Type: "string", Pattern: alternative.Pattern, MinLength: length}
					if !alternative.VariableLength {
						branch.MaxLength = &length
					}
					copy.AnyOf = append(copy.AnyOf, branch)
				}
				if schema.Nullable || domain.Nullable {
					copy.AnyOf = append(copy.AnyOf, &openapi3.Schema{Type: "string", Nullable: true, Enum: []any{nil}})
				}
				return &copy, nil
			}
		}
		result, _ := b.enumSchema(&copy, nil, enumeration.WireValues(input), 0)
		return result, nil
	}
	constraint, err := choice.Parse(field.Tag, field.Type)
	if err != nil || constraint == nil {
		return schema, err
	}
	values := constraint.WireValues()
	if quoted || schema.Type != constraint.JSONType() {
		return nil, fmt.Errorf("field %s choice conflicts with JSON wire type", field.Name)
	}
	result, _ := b.enumSchema(schema, nil, values, 0)
	return result, nil
}
