package builder

import (
	"fmt"

	"github.com/viant/datly/data"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/criteria"
)

// WithBuilderSelectorPresentation supplies verified, invocation-local public
// names for this exact view. It does not grant selection or filtering access.
func WithBuilderSelectorPresentation(fields []data.SelectorField) BuilderOption {
	detached := make([]data.SelectorField, len(fields))
	for i, field := range fields {
		detached[i] = field
		detached[i].Index = append([]int(nil), field.Index...)
	}
	return func(options *builderOptions) { options.selectorPresentation = detached }
}

func presentedOrderName(fields []data.SelectorField, name string) string {
	for _, field := range fields {
		if (dsql.ProjectionNames{field.PublicName}).Matches(name) {
			return field.GoName
		}
	}
	return name
}

// addPresentedCriteriaAliases runs after native Filterable policy restriction.
// Alias values reuse the exact allowed typed expression, including aggregate
// rewriting; no client text or SQL is rewritten.
func addPresentedCriteriaAliases(columns map[string]criteria.Column, fields []data.SelectorField) error {
	additions := map[string]criteria.Column{}
	for _, field := range fields {
		if field.Holder || field.OutputOnly {
			continue
		}
		var selected *criteria.Column
		for name, column := range columns {
			if !(dsql.ProjectionNames{field.GoName}).Matches(name) && (field.Column == "" || !(dsql.ProjectionNames{field.Column}).Matches(name)) {
				continue
			}
			if selected != nil && *selected != column {
				return fmt.Errorf("ambiguous criteria presentation field %q", field.PublicName)
			}
			copy := column
			selected = &copy
		}
		if selected == nil {
			continue
		}
		for name, column := range columns {
			if (dsql.ProjectionNames{field.PublicName}).Matches(name) && column != *selected {
				return fmt.Errorf("ambiguous criteria presentation field %q", field.PublicName)
			}
		}
		for name, column := range additions {
			if (dsql.ProjectionNames{field.PublicName}).Matches(name) && column != *selected {
				return fmt.Errorf("ambiguous criteria presentation field %q", field.PublicName)
			}
		}
		additions[field.PublicName] = *selected
	}
	for name, column := range additions {
		columns[name] = column
	}
	return nil
}
