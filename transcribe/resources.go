package transcribe

import (
	"io/fs"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

// resolveComponentSources expands embedded SQL for every view in a canonical
// component relation graph during the load/generation stage.
func resolveComponentSources(component *spec.Component, resources fs.FS) error {
	if component == nil {
		return nil
	}
	visited := map[*spec.View]bool{}
	var resolve func(*spec.View) error
	resolve = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		if err := dsql.ResolveSource(view.Name, view.Source, resources); err != nil {
			return err
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := resolve(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := resolve(component.RootView); err != nil {
		return err
	}
	for _, view := range component.Views {
		if err := resolve(view); err != nil {
			return err
		}
	}
	return nil
}
