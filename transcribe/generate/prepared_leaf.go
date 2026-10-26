package generate

import "github.com/viant/datly/spec"

// PreparedLeafPair identifies the two role declarations of a request leaf and
// its initialization-prepared write leaf. Shape and destination equality must
// still be verified by the generation planner before either can be emitted.
func PreparedLeafPair(root, first, second *spec.View) bool {
	if root == nil || !root.Auxiliary || first == nil || second == nil || first == second ||
		first.TypeName == "" || second.TypeName == "" || first.Auxiliary == second.Auxiliary ||
		first.InMemory || second.InMemory || len(first.Relations) != 0 || len(second.Relations) != 0 ||
		first.SelfReference != nil || second.SelfReference != nil {
		return false
	}
	public, physical := first, second
	if !public.Auxiliary {
		public, physical = physical, public
	}
	if public.WriterActionPolicy != "" || physical.WriterActionPolicy != "insert-delete" ||
		public.Source == nil || physical.Source == nil || public.Source.Table == "" || public.Source.Table != physical.Source.Table {
		return false
	}
	var sameParent func(*spec.View) bool
	visited := map[*spec.View]bool{}
	sameParent = func(parent *spec.View) bool {
		if parent == nil || !parent.Auxiliary || visited[parent] {
			return false
		}
		visited[parent] = true
		foundFirst, foundSecond := false, false
		for _, relation := range parent.Relations {
			if relation == nil || relation.Kind == spec.RelationKindDerived {
				continue
			}
			foundFirst = foundFirst || relation.View == first
			foundSecond = foundSecond || relation.View == second
		}
		if foundFirst && foundSecond {
			return true
		}
		for _, relation := range parent.Relations {
			if relation != nil && relation.Kind != spec.RelationKindDerived && sameParent(relation.View) {
				return true
			}
		}
		return false
	}
	return sameParent(root)
}
