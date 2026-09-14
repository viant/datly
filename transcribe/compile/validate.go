package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

func validateView(view *spec.View, visited map[*spec.View]bool) error {
	if view == nil {
		return fmt.Errorf("transcribe plan view is required")
	}
	if visited[view] {
		return nil
	}
	visited[view] = true
	for _, relation := range view.Relations {
		if relation == nil {
			return fmt.Errorf("view %s contains a nil relation", view.Name)
		}
		if strings.TrimSpace(relation.Name) == "" {
			return fmt.Errorf("view %s relation name is required", view.Name)
		}
		if relation.View == nil {
			return fmt.Errorf("view %s relation %s child view is required", view.Name, relation.Name)
		}
		if len(relation.On) == 0 && relation.Kind != spec.RelationKindDerived {
			return fmt.Errorf("view %s relation %s links are required", view.Name, relation.Name)
		}
		for _, link := range relation.On {
			if link == nil || strings.TrimSpace(link.ParentColumn) == "" || strings.TrimSpace(link.ChildColumn) == "" {
				return fmt.Errorf("view %s relation %s contains an incomplete link", view.Name, relation.Name)
			}
		}
		if err := validateView(relation.View, visited); err != nil {
			return err
		}
	}
	return nil
}

func validateViewNamespaces(root *spec.View) error {
	byName := map[string]*spec.View{}
	visited := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		name := strings.ToLower(strings.TrimSpace(view.Namespace))
		if name != "" {
			if existing := byName[name]; existing != nil && existing != view {
				return &Error{Code: CodeRelationAmbiguous, Cause: fmt.Errorf("compiled view namespace %q is not unique", view.Namespace)}
			}
			byName[name] = view
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := visit(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return visit(root)
}
