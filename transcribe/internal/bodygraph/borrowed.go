// Package bodygraph resolves canonical body paths without changing ownership.
package bodygraph

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

// ResolveBorrowed resolves exact generated holder names. An implicit writer body
// uses the component's name as its canonical default.
func ResolveBorrowed(c *spec.Component, path string) (*spec.View, []string, error) {
	view, graph, err := ResolveOccurrence(c, path)
	if err != nil {
		return nil, nil, err
	}
	if len(view.Relations) != 0 || view.SelfReference != nil {
		return nil, nil, fmt.Errorf("borrow_sql_row target is not a leaf")
	}
	return view, graph, nil
}

// ResolveOccurrence resolves the existing canonical body holders without granting
// generated-row ownership. Reference-only factory validation retains the complete
// graph; the generated borrower entry above continues to require a leaf.
func ResolveOccurrence(c *spec.Component, path string) (*spec.View, []string, error) {
	if c == nil || c.RootView == nil {
		return nil, nil, fmt.Errorf("borrow_sql_row requires a canonical body graph")
	}
	parts := strings.Split(path, "/")
	rootName := typecatalog.FieldName(c.Name)
	bodyCount := 0
	for _, p := range spec.EffectiveParameters(c.Parameters) {
		if p != nil && strings.EqualFold(p.Source.Kind, "body") {
			bodyCount++
			rootName = typecatalog.FieldName(p.Name)
		}
	}
	if bodyCount > 1 || parts[0] != rootName {
		return nil, nil, fmt.Errorf("borrow_sql_row body path %q does not select the exact canonical body holder %q", path, rootName)
	}
	view := c.RootView
	id, err := view.Identity()
	if err != nil {
		return nil, nil, err
	}
	graph := []string{id}
	seen := map[*spec.View]bool{view: true}
	for _, holder := range parts[1:] {
		var found *spec.View
		for _, relation := range view.Relations {
			if relation == nil || relation.View == nil {
				return nil, nil, fmt.Errorf("borrow_sql_row body relation is incomplete")
			}
			name := relation.Holder
			if name == "" {
				name = relation.Name
			}
			if typecatalog.FieldName(name) == holder {
				if found != nil {
					return nil, nil, fmt.Errorf("borrow_sql_row holder %q is ambiguous", holder)
				}
				found = relation.View
			}
		}
		if found == nil || seen[found] {
			return nil, nil, fmt.Errorf("borrow_sql_row holder %q is missing or cyclic", holder)
		}
		view = found
		seen[view] = true
		id, err = view.Identity()
		if err != nil {
			return nil, nil, err
		}
		graph = append(graph, id)
	}
	return view, graph, nil
}
