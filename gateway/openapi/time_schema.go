package openapi

import (
	"reflect"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
)

func timeInputSchema(field reflect.StructField) (*openapi3.Schema, error) {
	wire, err := registry.TimeInputSchema(field)
	if err != nil || wire == nil {
		return nil, err
	}
	result := &openapi3.Schema{}
	for _, alternative := range wire.AnyOf {
		result.AnyOf = append(result.AnyOf, &openapi3.Schema{Type: alternative.Type, Pattern: alternative.Pattern})
	}
	if wire.Nullable {
		result.AnyOf = append(result.AnyOf, &openapi3.Schema{Type: "string", Nullable: true, Enum: []any{nil}})
	}
	return result, nil
}
