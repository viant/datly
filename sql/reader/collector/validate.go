package collector

import "fmt"

// Validate verifies that every executable relation has a compiled row schema.
// A schema-less root is valid for components that expose only output relations.
func (g *Graph) Validate() error {
	if g == nil || g.Root == nil {
		return fmt.Errorf("collector graph root is required")
	}
	return validateView(g.Root, true, map[*View]bool{})
}

func validateView(view *View, root bool, visited map[*View]bool) error {
	if view == nil || view.View == nil {
		return fmt.Errorf("collector view metadata is required")
	}
	if visited[view] {
		return nil
	}
	visited[view] = true

	requiresSchema := !root || view.Spec.SelfReference != nil
	for _, relation := range view.Relations {
		if relation != nil && !relation.IsOutput() {
			requiresSchema = true
			break
		}
	}
	if requiresSchema && view.Schema.RowType() == nil {
		return fmt.Errorf("collector row type is required for view %s", viewName(view))
	}
	for _, relation := range view.Relations {
		if relation == nil || relation.Relation == nil {
			return fmt.Errorf("collector relation metadata is required for view %s", viewName(view))
		}
		if relation.Of == nil || relation.Of.View == nil {
			return fmt.Errorf("collector relation %s child view is required", relationName(relation))
		}
		if relation.Of.View.Schema.RowType() == nil {
			return fmt.Errorf("collector row type is required for relation %s child view %s", relationName(relation), viewName(relation.Of.View))
		}
		if err := validateView(relation.Of.View, false, visited); err != nil {
			return err
		}
	}
	return nil
}

func viewName(view *View) string {
	if view == nil || view.View == nil || view.Spec.Name == "" {
		return "<unnamed>"
	}
	return view.Spec.Name
}

func relationName(relation *Relation) string {
	if relation == nil || relation.Relation == nil || relation.Name == "" {
		return "<unnamed>"
	}
	return relation.Name
}
