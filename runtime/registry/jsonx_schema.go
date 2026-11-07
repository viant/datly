package registry

import (
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/jsonx"
	"reflect"
)

// JSONXInputSchema lowers the native decoder's compiled conversion domain.
// Array branches retain their consumer's existing item schema.
func JSONXInputSchema(field reflect.StructField) (*spec.WireSchema, error) {
	policy, err := jsonx.Compile(field.Tag, field.Type)
	if err != nil || !policy.Active() {
		return nil, err
	}
	domain := policy.InputDomain()
	result := &spec.WireSchema{Nullable: domain.Null}
	if domain.Integer || len(domain.IntegerValues) > 0 {
		branch := &spec.WireSchema{Type: "integer", Minimum: domain.Minimum, Maximum: domain.Maximum}
		for _, value := range domain.IntegerValues {
			branch.Enum = append(branch.Enum, value)
		}
		result.AnyOf = append(result.AnyOf, branch)
	}
	if domain.Boolean {
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "boolean"})
	}
	if domain.String || len(domain.StringValues) > 0 {
		branch := &spec.WireSchema{Type: "string"}
		for _, value := range domain.StringValues {
			branch.Enum = append(branch.Enum, value)
		}
		result.AnyOf = append(result.AnyOf, branch)
	}
	if domain.Number {
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "number"})
	}
	if domain.Array {
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "array"})
	}
	// null=omit alone does not replace the destination's normal representation.
	return result, nil
}
