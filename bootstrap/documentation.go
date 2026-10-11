package bootstrap

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

// documentationView retains the resolved occurrence graph and its compiled
// attribution without re-resolving resources or compiling another reader.
func documentationView(view *data.View) *spec.View {
	return documentationViewGraph(view, map[*data.View]*spec.View{})
}
func documentationViewGraph(view *data.View, seen map[*data.View]*spec.View) *spec.View {
	if view == nil {
		return nil
	}
	if result := seen[view]; result != nil {
		return result
	}
	value := view.Spec
	result := &value
	seen[view] = result
	if len(result.Columns) == 0 {
		for _, column := range view.Columns {
			if column != nil {
				result.Columns = append(result.Columns, &spec.Column{Name: column.Name, NameInferred: true, Source: column.Column, Output: column.Output, Tag: column.Tag})
			}
		}
	}
	result.Relations = nil
	for _, relation := range view.Relations {
		if relation == nil || relation.Of == nil {
			continue
		}
		result.Relations = append(result.Relations, &spec.Relation{Holder: relation.Holder, View: documentationViewGraph(relation.Of.View, seen)})
	}
	return result
}
