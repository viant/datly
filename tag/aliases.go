package tag

import (
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
)

// BindingAliases returns the logical and physical names for one bound field.
// A param source names another input datapoint and is therefore a dependency,
// not an alias owned by the derived destination field.
func BindingAliases(field reflect.StructField, param *spec.Parameter) []string {
	values := CanonicalBindingAliases(field, param)
	if param != nil && !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "param") {
		values = append(values, param.Source.Name)
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		key := strings.ToLower(strings.TrimSpace(value))
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, value)
	}
	return result
}

// CanonicalBindingAliases returns structural and logical names without flattening
// provider-specific transport locations into the explicit alias namespace.
func CanonicalBindingAliases(field reflect.StructField, param *spec.Parameter) []string {
	values := []string{field.Name}
	if param != nil {
		if param.QuerySelector != nil {
			values = append(values, param.QuerySelector.View+"."+param.Name)
		} else {
			values = append(values, param.Name)
		}
	}
	return values
}
