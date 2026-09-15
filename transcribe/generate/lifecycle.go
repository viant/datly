package generate

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

// ValidateLifecycleTarget checks dispatch ownership before artifact generation.
// Intermediate shape planning may precede mutation lowering; emitting an
// unsupported target must never silently retain an inert lifecycle declaration.
func (input *Input) ValidateLifecycleTarget(mutation bool) error {
	if input == nil || input.Component == nil {
		return nil
	}
	supported := mutation
	for _, route := range input.Component.Routes {
		if route != nil && !strings.EqualFold(route.Method, "POST") && !strings.EqualFold(route.Method, "PUT") && !strings.EqualFold(route.Method, "PATCH") {
			supported = false
		}
	}
	if supported {
		return nil
	}
	visited := map[*spec.View]bool{}
	var check func(*spec.View) error
	check = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		for _, column := range view.Columns {
			if column != nil && (column.DeleteMarker || column.ConcurrencyToken) {
				return fmt.Errorf("view %s: mutation markers require the generated Go mutation lifecycle", view.Name)
			}
		}
		if strings.TrimSpace(view.EntityHooks) != "" {
			return fmt.Errorf("view %s: lifecycle_type(%s, %q) requires the generated Go mutation lifecycle; readers use input_type OrdersInput.Init and output_type OrdersOutput.Finalize, with row OnFetch separate", view.Name, view.Name, view.EntityHooks)
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := check(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := check(input.Component.RootView); err != nil {
		return err
	}
	for _, view := range input.Component.Views {
		if err := check(view); err != nil {
			return err
		}
	}
	return nil
}
