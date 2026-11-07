package openapi

import (
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"reflect"
)

func jsonxInputSchema(field reflect.StructField, original *openapi3.Schema) (*openapi3.Schema, error) {
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
		if alternative.Type == "array" {
			if original.Items != nil {
				item := *original.Items
				item.Nullable = true
				branch.Items = &item
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
