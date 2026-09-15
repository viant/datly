package column

import (
	"fmt"
	"io/fs"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/tag"
)

// ValidateInvariants checks SQL view annotations against authoritative output
// names, supplied by the SQL projection or by SQLX column discovery.
func ValidateInvariants(view *spec.View, projected []string) error {
	if view == nil {
		return nil
	}
	for _, column := range view.Columns {
		if column == nil {
			continue
		}
		value, annotated := reflect.StructTag(column.Tag).Lookup(tag.InvariantName)
		if !annotated {
			continue
		}
		if _, err := tag.ParseInvariant(value); err != nil {
			return err
		}
		matches := 0
		for _, output := range projected {
			name := normalizedName(output)
			if name == normalizedName(column.Name) || (column.Source != "" && name == normalizedName(column.Source)) {
				matches++
			}
		}
		if matches == 0 {
			return fmt.Errorf("invariant target %s.%s is absent from the SQL projection", view.Namespace, column.Name)
		}
		if matches > 1 {
			return fmt.Errorf("invariant target %s.%s matches multiple projected columns", view.Namespace, column.Name)
		}
	}
	return nil
}

// ValidateSourceProjections is the no-database validation path. An unresolved
// wildcard needs discovery; authored annotations are not evidence of outputs.
func (r *Refiner) ValidateSourceProjections(component *spec.Component, resources fs.FS) error {
	if component == nil {
		return fmt.Errorf("transcribe column: component is required")
	}
	visited := map[*spec.View]bool{}
	var validate func(*spec.View) error
	validate = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		annotated := false
		for _, column := range view.Columns {
			if column != nil {
				if _, ok := reflect.StructTag(column.Tag).Lookup(tag.InvariantName); ok {
					annotated = true
					break
				}
			}
		}
		if annotated && view.Source != nil {
			source := view.Source.Clone()
			if err := dsql.ResolveSource(view.Name, source, resources); err != nil {
				return err
			}
			SQL := strings.TrimSpace(source.SQL)
			if SQL == "" && source.Table != "" {
				SQL = "SELECT * FROM " + source.Table
			}
			if SQL != "" {
				columns, err := (dsql.SelectorProjection{SQL: SQL}).Columns(nil)
				if err != nil {
					return fmt.Errorf("invariant projection for view %s requires column discovery: %w", view.Namespace, err)
				}
				names := make([]string, 0, len(columns))
				for _, column := range columns {
					names = append(names, column.OutputName())
				}
				if err := ValidateInvariants(view, names); err != nil {
					return err
				}
			}
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := validate(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := validate(component.RootView); err != nil {
		return err
	}
	for _, view := range component.Views {
		if err := validate(view); err != nil {
			return err
		}
	}
	return nil
}
