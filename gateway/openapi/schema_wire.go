package openapi

import (
	"crypto/sha256"
	"fmt"
	"reflect"

	"github.com/viant/datly/gateway/openapi/openapi3"
	jsonmarshal "github.com/viant/structology/encoding/json/marshal"
)

// wire lowers serializer-owned facts to OpenAPI. No Go fields, tags, exclusions,
// naming or omission policies are interpreted in this path.
func (b *schemaBuilder) wire(shape *jsonmarshal.WireShape) (*openapi3.Schema, error) {
	key := wireKey{shape: shape, path: b.path}
	if existing := b.activeWires[shape]; existing != nil {
		return existing, nil
	}
	if existing := b.wires[key]; existing != nil {
		return existing, nil
	}
	if shape.Kind() == reflect.Pointer {
		element, err := b.wire(shape.Element())
		if err != nil {
			return nil, err
		}
		return b.nullable(element), nil
	}
	result := &openapi3.Schema{Nullable: shape.Nullable(), Format: shape.Format()}
	switch shape.Kind() {
	case reflect.Struct:
		name := fmt.Sprintf("Wire_%x", sha256.Sum256([]byte(b.scope+":"+b.path+":"+shape.Source().String())))
		ref := &openapi3.Schema{Ref: "#/components/schemas/" + name}
		b.wires[key] = ref
		b.activeWires[shape] = ref
		defer delete(b.activeWires, shape)
		b.schemas[name] = result
		result.Type = "object"
		result.Properties = openapi3.Schemas{}
		for _, field := range shape.Properties() {
			parent := b.path
			if b.path != "" {
				b.path += "."
			}
			b.path += field.Field().Name
			annotation := b.docs.StructField(b.path, field.Field())
			property, err := b.wire(field.Shape())
			b.path = parent
			if err != nil {
				return nil, err
			}
			if annotation.Description != "" || annotation.Example != "" {
				copy := *property
				property = &copy
				if property.Ref != "" {
					property = &openapi3.Schema{AllOf: openapi3.SchemaList{property}}
				}
				property.Description = annotation.Description
				if annotation.Example != "" {
					property.Example = annotation.Example
				}
			}
			result.Properties[field.Name()] = property
			if field.Required() {
				result.Required = append(result.Required, field.Name())
			}
		}
		if result.Nullable {
			// Keep nullable outside the referenced object so strict 3.0 consumers accept null.
			result.Nullable = false
			nullable := b.nullable(ref)
			b.wires[key] = nullable
			return nullable, nil
		}
		return ref, nil
	case reflect.Slice, reflect.Array:
		result.Type = "array"
		element, err := b.wire(shape.Element())
		if err != nil {
			return nil, err
		}
		result.Items = element
		if shape.Length() >= 0 {
			size := uint64(shape.Length())
			result.MinItems = size
			result.MaxItems = &size
		}
	case reflect.Map:
		result.Type = "object"
		element, err := b.wire(shape.Element())
		if err != nil {
			return nil, err
		}
		result.AdditionalProperties = element
	case reflect.Interface:
		return &openapi3.Schema{}, nil
	case reflect.String:
		result.Type = "string"
	case reflect.Bool:
		result.Type = "boolean"
	case reflect.Int8, reflect.Int16, reflect.Int32:
		result.Type = "integer"
		result.Format = "int32"
	case reflect.Int, reflect.Int64:
		result.Type = "integer"
		result.Format = "int64"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		result.Type = "integer"
		zero := float64(0)
		result.Min = &zero
	case reflect.Float32:
		result.Type = "number"
		result.Format = "float"
	case reflect.Float64:
		result.Type = "number"
		result.Format = "double"
	default:
		return nil, fmt.Errorf("unsupported native JSON wire kind %s", shape.Kind())
	}
	return result, nil
}
