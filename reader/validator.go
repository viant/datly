package reader

import (
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/sanitize"
	"strings"
)

func ValidateSessionSelectors(s *Session) error {
	if len(s.Selectors) == 0 {
		return nil
	}

	var view *data.View
	var err error
	sb := &strings.Builder{}
	for viewName, selector := range s.Selectors {
		view, err = s.View.AnyOfViews(viewName)
		if err != nil {
			return err
		}

		if err = validateOffset(selector, view); err != nil {
			return err
		}

		if err = validateLimit(selector, view); err != nil {
			return err
		}

		err = sanitizeCriteria(selector, view, err, sb)
		if err != nil {
			return err
		}

		err = sanitizeOrderBy(selector, view, sb)
		if err != nil {
			return err
		}

		if err = sanitizeColumns(selector, view, sb); err != nil {
			return err
		}

	}
	return nil
}

func validateLimit(selector *data.Selector, view *data.View) error {
	if selector.Limit == 0 {
		return nil
	}

	if view.CanUseSelectorLimit() {
		return nil
	}

	return fmt.Errorf("it is not allowed to use selector limit on view %v", view.Name)

}

func validateOffset(selector *data.Selector, view *data.View) error {
	if selector.Offset == 0 {
		return nil
	}

	if view.CanUseSelectorOffset() {
		return nil
	}

	return fmt.Errorf("it is not allowed to use selector offset on view %v", view.Name)
}

func sanitizeColumns(selector *data.Selector, view *data.View, sb *strings.Builder) error {
	if selector.Columns == nil {
		return nil
	}

	for i, column := range selector.Columns {
		viewColumn, ok := view.ColumnByName(column)
		if !ok {
			return fmt.Errorf("not found column %v in view %v", column, view.Name)
		}

		if !viewColumn.Filterable {
			return fmt.Errorf("column %v is not filterable", column)
		}

		node, err := sanitize.Parse([]byte(column))
		if err != nil {
			return err
		}

		err = node.Sanitize(sb, view.IndexedColumns())
		if err != nil {
			return err
		}

		selector.Columns[i] = sb.String()
		sb.Reset()
	}

	return nil
}

func sanitizeCriteria(selector *data.Selector, view *data.View, err error, sb *strings.Builder) error {
	if selector.Criteria != nil && !view.SelectorConstraints.Criteria {
		return fmt.Errorf("it is not allowed to use selector criteria on view %v", view.Name)
	}

	if selector.Criteria == nil {
		return nil
	}

	node, err := sanitize.Parse([]byte(selector.Criteria.Expression))
	if err != nil {
		return err
	}

	err = node.Sanitize(sb, view.IndexedColumns())
	if err != nil {
		return err
	}

	selector.Criteria.Expression = sb.String()
	sb.Reset()
	return nil
}

func sanitizeOrderBy(selector *data.Selector, view *data.View, sb *strings.Builder) error {
	if selector.OrderBy != "" && !view.CanUseSelectorOrderBy() {
		return fmt.Errorf("it is not allowed to use selector order by on view %v", view.Name)
	}

	if selector.OrderBy == "" {
		return nil
	}

	node, err := sanitize.Parse([]byte(selector.OrderBy))
	if err != nil {
		return err
	}

	err = node.Sanitize(sb, view.IndexedColumns())
	if err != nil {
		return err
	}
	selector.OrderBy = sb.String()
	sb.Reset()

	return nil
}
