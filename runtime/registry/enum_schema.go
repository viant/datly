package registry

import (
	"encoding/json"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	"math/big"
)

// EnumInputSchema describes the same accepted domain used by native binding.
func EnumInputSchema(enumeration *choice.Enum, nullable bool) *spec.WireSchema {
	if collection := enumeration.CollectionItems(); collection != nil {
		return &spec.WireSchema{Type: "array", Items: &spec.WireSchema{Type: "string", Enum: collection.WireValues()}, Nullable: enumeration.AllowsNullInput()}
	}
	result := &spec.WireSchema{Type: enumeration.WireType(true), Enum: enumeration.WireValues(true), Nullable: nullable && enumeration.AllowsNullInput()}
	domain := enumeration.InputDomain()
	if domain == nil {
		return result
	}
	result.Type, result.Enum = "", nil
	result.Nullable = (nullable || domain.Nullable) && enumeration.AllowsNullInput()
	if domain.OpenIntegers {
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "integer", Minimum: ExactSchemaIntegerBound(domain.Minimum), Maximum: ExactSchemaIntegerBound(domain.Maximum)})
	} else if len(domain.Integers) > 0 {
		values := make([]any, len(domain.Integers))
		for i, value := range domain.Integers {
			values[i] = value
		}
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "integer", Enum: values})
	}
	if domain.OpenStrings {
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "string"})
	} else if len(domain.Strings) > 0 {
		values := make([]any, len(domain.Strings))
		for i, value := range domain.Strings {
			values[i] = value
		}
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "string", Enum: values})
	}
	for _, alternative := range domain.StringAlternatives {
		length := alternative.Length
		branch := &spec.WireSchema{Type: "string", Pattern: alternative.Pattern, MinLength: &length}
		if !alternative.VariableLength {
			branch.MaxLength = &length
		}
		result.AnyOf = append(result.AnyOf, branch)
	}
	return result
}

// ExactSchemaIntegerBound preserves integer bounds through JSON serialization.
func ExactSchemaIntegerBound(value json.Number) *json.Number {
	_, ok := new(big.Int).SetString(string(value), 10)
	if !ok {
		return nil
	}
	return &value
}
