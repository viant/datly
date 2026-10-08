package compile

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"io/fs"
	"strings"
)

// BackfillDocumentationMetadata resolves dictionary lineage once during
// authoring. Linked loading consumes the persisted origins, including unknowns.
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
		complete := view.DocumentationTable != ""
		for _, column := range view.Columns {
			if column != nil && column.DocumentationOrigin == nil {
				complete = false
			}
		}
		if !complete {
			resolved := data.FromView(nil, view)
			ready, err := resolveMetadataSQL(resolved, resources)
			if err != nil {
				return err
			}
			if ready && resolved.Spec.Source != nil && strings.TrimSpace(resolved.Spec.Source.SQL) != "" {
				// Templates or opaque vendor SQL may not expose a statically proven lineage.
				// Preserve their authored table metadata and defer query validation to its owner.
				if parsed, err := sqlparser.ParseQuery(resolved.Spec.Source.SQL); err == nil && parsed != nil {
					lineage := (sqlparser.Lineage{Query: parsed}).Compile()
					if view.DocumentationTable == "" {
						view.DocumentationTable = view.Source.Table
						if view.DocumentationTable == "" {
							view.DocumentationTable = lineage.RootTable()
						}
					}
					for _, column := range view.Columns {
						if column == nil || column.DocumentationOrigin != nil {
							continue
						}
						column.DocumentationOrigin = &spec.ColumnOrigin{}
						name := column.Output
						if name == "" {
							name = column.Source
						}
						if name == "" {
							name = column.Name
						}
						if origin, ok := lineage.Lookup(name); ok {
							column.DocumentationOrigin.Table = origin.Table
							column.DocumentationOrigin.Column = origin.Column
						}
					}
				}
			}
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
