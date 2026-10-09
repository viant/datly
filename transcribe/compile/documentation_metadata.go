package compile

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"io/fs"
)

// BackfillDocumentationMetadata derives transient occurrence lineage during authoring.
func BackfillDocumentationMetadata(component *spec.Component, resources ...fs.FS) error {
	if component == nil {
		return nil
	}
	seen := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || seen[view] {
			return nil
		}
		seen[view] = true
		resolved := data.FromView(nil, view)
		ready, err := resolveMetadataSQL(resolved, resources)
		if err != nil {
			return err
		}
		if !ready {
			original := view.Source
			view.Source = nil
			view.CompileProjectionOrigins()
			view.Source = original
		}
		if ready {
			original := view.Source
			view.Source = resolved.Spec.Source
			view.CompileProjectionOrigins()
			view.Source = original
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
	if err := visit(component.RootView); err != nil {
		return err
	}
	for _, view := range component.Views {
		if err := visit(view); err != nil {
			return err
		}
	}
	return nil
}
