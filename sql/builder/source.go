package builder

import (
	"fmt"
	"strings"

	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlparser"
)

func (b *Builder) resolveSourceSQL(options *builderOptions) error {
	if options == nil || strings.TrimSpace(options.sqlText) != "" {
		return nil
	}
	if options.source == nil || strings.TrimSpace(options.source.Table) == "" {
		return nil
	}
	if options.view == nil || len(options.view.Columns) == 0 {
		return fmt.Errorf("table source %q requires static view columns", options.source.Table)
	}
	columns := make([]string, 0, len(options.view.Columns))
	allowNulls := options.view.NullsAllowed()
	for _, column := range options.view.Columns {
		if column == nil || strings.TrimSpace(column.SelectExpression(allowNulls)) == "" {
			continue
		}
		resolved, err := (dsql.SelectorProjection{View: options.view}).TableColumn(column)
		if err != nil {
			return err
		}
		columns = append(columns, resolved.SelectExpression(allowNulls))
	}
	if len(columns) == 0 {
		return fmt.Errorf("table source %q has no selectable columns", options.source.Table)
	}
	candidate := "SELECT " + strings.Join(columns, ", ") + " FROM " + strings.TrimSpace(options.source.Table)
	// Relation filters retain the canonical DQL namespace. A table-only
	// source must declare that alias just as an authored SELECT source does.
	namespace := strings.TrimSpace(options.view.Spec.Namespace)
	if options.relation != nil && options.relation.Of != nil {
		for _, link := range options.relation.Of.On {
			if link == nil || strings.TrimSpace(link.Namespace) == "" {
				continue
			}
			linked := strings.TrimSpace(link.Namespace)
			if namespace != "" && namespace != linked {
				return fmt.Errorf("table source %q has conflicting relation namespaces %q and %q", options.source.Table, namespace, linked)
			}
			namespace = linked
		}
	}
	if namespace != "" && namespace != strings.TrimSpace(options.source.Table) {
		candidate += " " + namespace
	}
	if _, err := sqlparser.ParseQuery(candidate); err != nil {
		return fmt.Errorf("build SQL from table source %q: %w", options.source.Table, err)
	}
	options.sqlText = candidate
	options.tableProjection = true
	return nil
}
