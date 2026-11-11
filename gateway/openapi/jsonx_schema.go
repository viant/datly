package openapi

import (
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"reflect"
)

func (b *schemaBuilder) jsonxInputSchema(field reflect.StructField, original *openapi3.Schema) (*openapi3.Schema, error) {
	wire, err := registry.JSONXInputSchema(field)
	if err != nil || wire == nil {
		return original, err
	}
	if len(wire.AnyOf) == 0 {
		if original.Ref != "" || original.Type == "" {
			return &openapi3.Schema{AnyOf: openapi3.SchemaList{original, {Type: "object", Nullable: true, Enum: []any{nil}}}}, nil
		}
		copy := *original
		copy.Nullable = wire.Nullable
		if len(copy.Enum) > 0 {
			copy.Enum = append(append([]any(nil), copy.Enum...), nil)
		}
		return &copy, nil
	}
	result := &openapi3.Schema{Description: original.Description, Example: original.Example}
	for i, alternative := range wire.AnyOf {
		branch := &openapi3.Schema{Type: alternative.Type, Enum: alternative.Enum, Min: alternative.Minimum, Max: alternative.Maximum}
		if alternative.Type == "object" && original.Items != nil {
			item := *original.Items
			branch = &item
		}
		if alternative.Type == "array" {
			if original.Items != nil {
				branch.Items = b.nullable(original.Items)
			}
		}
		// OpenAPI 3.0 expresses null as a nullable typed branch.
		if i == 0 && wire.Nullable {
			branch.Nullable = true
			if len(branch.Enum) > 0 {
				branch.Enum = append(append([]any(nil), branch.Enum...), nil)
			}
		}
		result.AnyOf = append(result.AnyOf, branch)
	}
	return result, nil
}

// strictInputValue removes only root value nullability. Array items and object
// properties retain their independent null contracts and shared references.
func strictInputValue(schema *openapi3.Schema, requireRecord bool) *openapi3.Schema {
	if schema == nil {
		return nil
	}
	result := *schema
	result.Nullable = false
	if requireRecord && result.Type == "array" && result.MinItems < 1 {
		result.MinItems = 1
	}
	if len(schema.Enum) > 0 {
		result.Enum = make([]any, 0, len(schema.Enum))
		for _, value := range schema.Enum {
			if value != nil {
				result.Enum = append(result.Enum, value)
			}
		}
	}
	for _, pair := range []struct {
		source openapi3.SchemaList
		target *openapi3.SchemaList
	}{
		{schema.AnyOf, &result.AnyOf}, {schema.OneOf, &result.OneOf}, {schema.AllOf, &result.AllOf},
	} {
		if len(pair.source) == 0 {
			continue
		}
		branches := make(openapi3.SchemaList, 0, len(pair.source))
		for _, branch := range pair.source {
			nullOnly := branch != nil && len(branch.Enum) > 0
			if nullOnly {
				for _, value := range branch.Enum {
					if value != nil {
						nullOnly = false
						break
					}
				}
			}
			if nullOnly {
				continue
			}
			branches = append(branches, strictInputValue(branch, requireRecord))
		}
		*pair.target = branches
	}
	return &result
}
