package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

// SchemaInspection records guaranteed stages of successful compiler refinement.
// It is not validation of a runtime payload or of every SQL template branch.
type SchemaInspection struct {
	Component string   `json:"component"`
	View      string   `json:"view"`
	Connector string   `json:"connector"`
	Table     string   `json:"table,omitempty"`
	Completed []string `json:"completed"`
}

// Discovery returns a project only after all configured refinement calls pass.
// Describe only the source-bearing views that the refiner actually visits.
func (r *ValidationReport) schemaDiscovery(project *ProjectGeneration) error {
	for _, result := range project.Components {
		component := result.Component
		connector := ""
		if component.Settings != nil {
			connector = strings.TrimSpace(component.Settings.DefaultConnector)
		}
		visited := map[*spec.View]bool{}
		var visit func(*spec.View, string) error
		visit = func(view *spec.View, inherited string) error {
			if view == nil || visited[view] {
				return nil
			}
			visited[view] = true
			if view.Source != nil && view.Source.Bindings != nil && strings.TrimSpace(view.Source.Bindings.Connector) != "" {
				inherited = strings.TrimSpace(view.Source.Bindings.Connector)
			}
			source := view.Source.Clone()
			if err := dsql.ResolveSource(view.Name, source, result.Source.Resources); err != nil {
				return err
			}
			if source != nil && (strings.TrimSpace(source.SQL) != "" || strings.TrimSpace(source.Table) != "") {
				inspection := SchemaInspection{Component: component.Key.String(), View: view.Name,
					Connector: inherited, Table: strings.TrimSpace(source.Table), Completed: []string{"SQLX dialect resolution", "compile-time SQL source refinement"}}
				if inspection.Table != "" {
					inspection.Completed = append(inspection.Completed, "SQLX query column discovery", "SQLX explicit-table metadata lookup")
				} else {
					// Query-only templates can evaluate to empty SQL. The existing
					// refiner does not expose per-query evidence, so do not infer it.
					r.Skipped = append(r.Skipped, fmt.Sprintf("column/table metadata coverage for %s view %s (query-only evaluated-query evidence unavailable)", component.Key.String(), view.Name))
				}
				r.Schema = append(r.Schema, inspection)
			} else {
				r.Skipped = append(r.Skipped, fmt.Sprintf("database discovery for %s view %s (no SQL/table source)", component.Key.String(), view.Name))
			}
			for _, relation := range view.Relations {
				if relation != nil {
					if err := visit(relation.View, inherited); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if err := visit(component.RootView, connector); err != nil {
			return err
		}
		for _, view := range component.Views {
			if err := visit(view, connector); err != nil {
				return err
			}
		}
	}
	if len(r.Schema) > 0 {
		r.Completed = append(r.Completed, "database source refinement for listed schema sources")
	} else {
		r.Skipped = append(r.Skipped, "database column discovery (no SQL/table sources)")
	}
	return nil
}
