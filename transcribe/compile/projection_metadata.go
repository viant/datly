package compile

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/builder"
)

// AuthoredViewProjection resolves canonical column metadata against reader SQL outputs.
// It compiles names only; row binding and invocation execution stay reader-owned.
type AuthoredViewProjection struct{ View *spec.View }

// AuthoredOutputColumn pairs canonical role/type metadata with proven selection and SQL
// names. A discovered Go name is never selector alias authority.
type AuthoredOutputColumn struct {
	Column   *spec.Column
	Name     string
	Selector string
}

func (p AuthoredViewProjection) Columns() ([]AuthoredOutputColumn, error) {
	view := data.FromView(nil, p.View)
	if view == nil || view.Spec.Source == nil {
		return nil, fmt.Errorf("source projection is required")
	}
	source := view.Spec.Source.SQL
	if strings.TrimSpace(source) == "" && view.Spec.Source.Table != "" {
		// Table rendering remains with the same owner used during execution.
		query, err := builder.NewBuilder().Build(context.Background(), builder.WithBuilderView(view))
		if err != nil {
			return nil, err
		}
		source = query.SQL
	}
	outputs, err := (dsql.SelectorProjection{SQL: source, View: view}).Columns(nil)
	if err != nil {
		return nil, err
	}
	resolver := authoredOutputProjection{outputs: outputs}
	var result []AuthoredOutputColumn
	var names dsql.ProjectionNames
	for _, column := range view.Spec.Columns {
		if column == nil || strings.TrimSpace(column.Name) == "" {
			continue
		}
		field, err := resolver.resolve(column)
		if err != nil {
			return nil, err
		}
		if names.Matches(field.Name) {
			return nil, fmt.Errorf("duplicate projected column metadata %q; assign distinct SQL aliases", field.Name)
		}
		names = append(names, field.Name)
		result = append(result, field)
	}
	return result, nil
}

type authoredOutputProjection struct{ outputs []dsql.ProjectionColumn }

func (p authoredOutputProjection) resolve(column *spec.Column) (AuthoredOutputColumn, error) {
	match := -1
	for i, output := range p.outputs {
		if !output.MatchesOutput(column.Source) && (column.NameInferred || !output.MatchesOutput(column.Name)) {
			continue
		}
		if match >= 0 {
			return AuthoredOutputColumn{}, fmt.Errorf("ambiguous column %q source %q", column.Name, column.Source)
		}
		match = i
	}
	if match < 0 {
		return AuthoredOutputColumn{}, fmt.Errorf("column %q source %q has no declared output", column.Name, column.Source)
	}
	output := p.outputs[match]
	name := output.OutputName()
	if !column.NameInferred && output.Matches(column.Name) {
		for i, other := range p.outputs {
			if i != match && other.Matches(column.Name) {
				return AuthoredOutputColumn{}, fmt.Errorf("ambiguous column mapping %q", column.Name)
			}
		}
		// Matching output spellings retain the declared SQL spelling. Only a
		// distinct, authored mapping needs a separate selector name.
		if !output.MatchesOutput(column.Name) {
			name = column.Name
		}
	}
	return AuthoredOutputColumn{Column: column, Name: output.OutputName(), Selector: name}, nil
}

// BackfillReportMetadata records parsed output labels before generated shapes
// cross the linked load boundary. Non-report components do not need this pass.
func BackfillReportMetadata(component *spec.Component, resources ...fs.FS) error {
	if component == nil || component.Settings == nil || component.Settings.Report == nil || !component.Settings.Report.Enabled || component.RootView == nil || len(component.RootView.Columns) == 0 {
		return nil
	}
	complete := true
	for _, column := range component.RootView.Columns {
		if column != nil && (column.Output == "" || column.Selector == "") {
			complete = false
			break
		}
	}
	if complete {
		return nil
	}
	view := data.FromView(nil, component.RootView)
	if err := resolveMetadataSQL(view, resources); err != nil {
		return err
	}
	projected, err := (AuthoredViewProjection{View: &view.Spec}).Columns()
	if err != nil {
		return err
	}
	for _, column := range component.RootView.Columns {
		if column == nil {
			continue
		}
		for _, output := range projected {
			if output.Column.Name == column.Name && output.Column.Source == column.Source {
				column.Output, column.Selector = output.Name, output.Selector
				break
			}
		}
	}
	return nil
}
