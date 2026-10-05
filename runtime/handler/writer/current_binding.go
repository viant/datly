package writer

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/typecatalog"
)

// Resolve role ownership once at compilation. An auxiliary lookup of the same
// table is evidence, never an implicit replacement for a writable role's
// Previous. Prefer the role's named Current over a table-only fallback.
func currentInputField(input reflect.Type, table string, auxiliary bool, roles ...string) (int, reflect.Type, error) {
	selected, priority := -1, len(roles)+1
	var selectedType reflect.Type
	ambiguous := ""
	for i := 0; i < input.NumField(); i++ {
		field := input.Field(i)
		if field.Type.Kind() != reflect.Slice && field.Type.Kind() != reflect.Pointer || !strings.Contains(field.Tag.Get("parameter"), "kind=view") {
			continue
		}
		candidate := dereference(field.Type)
		if candidate == nil || candidate.Kind() != reflect.Struct {
			continue
		}
		view := field.Tag.Get("view")
		readOnly := strings.EqualFold(tagOption(view, "auxiliary"), "true")
		rank := len(roles) + 1
		for j, role := range roles {
			if strings.EqualFold(strings.TrimSuffix(field.Name, "View"), "Current"+typecatalog.FieldName(strings.TrimSuffix(role, "View"))) && (!readOnly || auxiliary) {
				rank = j
				break
			}
		}
		if rank == len(roles)+1 && !readOnly && table != "" && strings.EqualFold(tagOption(view, "table"), table) {
			rank = len(roles)
		}
		if rank > priority || rank == len(roles)+1 {
			continue
		}
		if rank == priority {
			ambiguous = field.Name
			continue
		}
		selected, selectedType, priority = i, candidate, rank
		ambiguous = ""
	}
	if ambiguous != "" {
		return -1, nil, fmt.Errorf("ambiguous current-state inputs %s and %s for %s", input.Field(selected).Name, ambiguous, table)
	}
	return selected, selectedType, nil
}
