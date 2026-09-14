package generate

import (
	"strings"

	"github.com/viant/datly/spec"
)

func resolveHelperTypes(component *spec.Component, declarations Declarations) []HelperType {
	if component == nil {
		return nil
	}
	var result []HelperType
	seen := map[string]bool{}
	for _, param := range preferDefinedParams(component.Parameters) {
		if param == nil {
			continue
		}
		helperType, ok := inferHelperType(param, declarations)
		if !ok || seen[helperType] {
			continue
		}
		fields := helperFields(param, declarations)
		if len(fields) == 0 {
			continue
		}
		seen[helperType] = true
		result = append(result, HelperType{Name: helperTypeName(helperType), Fields: fields})
	}
	return result
}

func inferHelperType(param *spec.Parameter, declarations Declarations) (string, bool) {
	if param == nil {
		return "", false
	}
	if !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "param") {
		return "", false
	}
	if strings.TrimSpace(param.TypeExpr) != "" || strings.TrimSpace(param.OutputTypeExpr) != "" {
		return "", false
	}
	if defaultTagTypeName(param.Tag) != "" {
		return "", false
	}
	name := exportedName(strings.TrimSpace(param.Name))
	if name == "" {
		return "", false
	}
	declaration := declarations.declaration(param)
	if declaration.isAggregate() {
		return name + "Helper", true
	}
	if len(declaration.Projection) > 0 {
		return "[]" + name + "Row", true
	}
	return "", false
}

func helperTypeName(typeExpr string) string {
	typeExpr = strings.TrimSpace(typeExpr)
	return strings.TrimPrefix(typeExpr, "[]")
}

// helperFields returns parser-proven destination fields for a row projection
// or the single authored destination alias for an ARRAY_AGG projection.
func helperFields(param *spec.Parameter, declarations Declarations) []Field {
	if param == nil {
		return nil
	}
	if fields := rowHelperFields(param, declarations); len(fields) > 0 {
		return fields
	}
	declaration := declarations.declaration(param)
	if declaration.isAggregate() {
		return []Field{{Name: strings.TrimSpace(declaration.Projection[0].Name)}}
	}
	return nil
}

func rowHelperFields(param *spec.Parameter, declarations Declarations) []Field {
	if param == nil {
		return nil
	}
	declaration := declarations.declaration(param)
	if len(declaration.Projection) == 0 || declaration.isAggregate() {
		return nil
	}
	result := make([]Field, 0, len(declaration.Projection))
	for _, projected := range declaration.Projection {
		name := strings.TrimSpace(projected.Name)
		if name == "" {
			continue
		}
		result = append(result, Field{Name: name})
	}
	return result
}

func (d Declaration) isAggregate() bool {
	return len(d.Projection) == 1 && d.Projection[0].Aggregate
}

func inferFieldType(param *spec.Parameter, declarations Declarations) string {
	if param == nil {
		return "any"
	}
	if strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") && strings.EqualFold(strings.TrimSpace(param.Source.Name), "view") {
		if strings.EqualFold(strings.TrimSpace(param.Cardinality), "One") {
			return "*View"
		}
		return "[]*View"
	}
	if dataType := declarations.declaration(param).DataType; dataType != "" {
		return dataType
	}
	return "any"
}
