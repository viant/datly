package registry

import (
	"encoding/json"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	"math/big"
)

// EnumInputSchema describes the same accepted domain used by native binding.
func EnumInputSchema(enumeration *choice.Enum, nullable bool) *spec.WireSchema {
	result := &spec.WireSchema{Type: enumeration.WireType(true), Enum: enumeration.WireValues(true), Nullable: nullable}
	domain := enumeration.InputDomain()
	if domain == nil {
		return result
	}
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
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "string", Pattern: alternative.Pattern, MinLength: &length, MaxLength: &length})
	}
	return result
}

// ExactSchemaIntegerBound avoids narrowing accepted integers through float64 rounding.
func ExactSchemaIntegerBound(value json.Number) *float64 {
	integer, ok := new(big.Int).SetString(string(value), 10)
	if !ok {
		return nil
	}
	bound, accuracy := new(big.Float).SetInt(integer).Float64()
	if accuracy != big.Exact {
		return nil
	}
	return &bound
}
