package bootstrap

import "github.com/viant/datly/spec"

// ReportSourceComponent returns detached projection metadata for report
// derivation. Relation tags are resolved by the reader compiler, not necessarily
// present in the authored spec. This snapshot does not replace execution plans.
func (a *Artifact) ReportSourceComponent() *spec.Component {
	if a == nil || a.Component == nil {
		return nil
	}
	result := a.Component.Clone()
	if result.RootView == nil || a.Reader == nil || a.Reader.Root == nil {
		return result
	}
	root := a.Reader.Root
	result.RootView.Source = root.View.Spec.Source.Clone()
	result.RootView.Relations = nil
	for _, edge := range root.Relations {
		relation := edge.Relation
		if relation.IsOutput() {
			continue
		}
		projected := &spec.Relation{
			Name: relation.Name, Kind: relation.Kind, Holder: relation.Holder,
			Cardinality: relation.Cardinality, View: edge.Target.View.Spec.Clone(),
		}
		for i, parent := range relation.On {
			child := relation.Of.On[i]
			projected.On = append(projected.On, &spec.RelationLink{
				ParentNamespace: parent.Namespace, ParentColumn: parent.OutputColumn(),
				ChildNamespace: child.Namespace, ChildColumn: child.OutputColumn(),
			})
		}
		result.RootView.Relations = append(result.RootView.Relations, projected)
	}
	return result
}
