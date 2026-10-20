package bodygraph

import "github.com/viant/datly/spec"

// AllAuxiliary recognizes a complete concrete body tree without granting DML
// ownership. Pending SQL does not establish auxiliary intent at this stage.
func AllAuxiliary(root *spec.View) bool {
	seen := map[*spec.View]bool{}
	var visit func(*spec.View) bool
	visit = func(view *spec.View) bool {
		if view == nil || seen[view] || !view.Auxiliary || view.InMemory || view.SelfReference != nil {
			return false
		}
		seen[view] = true
		for _, relation := range view.Relations {
			if relation == nil || relation.Kind == spec.RelationKindDerived || !visit(relation.View) {
				return false
			}
		}
		return true
	}
	return visit(root)
}
