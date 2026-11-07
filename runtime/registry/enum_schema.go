package registry

import (
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
)

// EnumInputSchema describes the same accepted domain used by native binding.
func EnumInputSchema(enumeration *choice.Enum, nullable bool) *spec.WireSchema {
	result := &spec.WireSchema{Type: enumeration.WireType(true), Enum: enumeration.WireValues(true), Nullable: nullable}
	domain := enumeration.InputDomain()
	if domain == nil {
		return result
	}
	if len(domain.Integers) > 0 {
		values := make([]any, len(domain.Integers))
		for i, value := range domain.Integers {
			values[i] = value
		}
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "integer", Enum: values})
	}
	if len(domain.Strings) > 0 {
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
