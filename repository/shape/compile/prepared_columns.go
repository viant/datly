package compile

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/column"
	"github.com/viant/datly/repository/shape/plan"
)

func applyPreparedColumns(ctx context.Context, source *shape.Source, result *plan.Result, resolve column.PreparedResolver) error {
	for _, item := range result.Views {
		if item == nil || !isQueryLikeMode(item.Mode) {
			continue
		}
		if strings.TrimSpace(item.Summary) != "" || strings.TrimSpace(item.SummaryURL) != "" {
			return fmt.Errorf("prepared columns %s: summary projections are not supported", item.Name)
		}
		sql := item.SQL
		if strings.TrimSpace(sql) == "" {
			if item.SQLURI != "" {
				return fmt.Errorf("prepared columns %s: external SQL URI without inline SQL is unsupported", item.Name)
			}
			if item.Table == "" {
				continue
			}
			sql = "SELECT * FROM " + item.Table
		}
		expanded, err := expandPreparedSQL(source, sql, map[string]bool{})
		if err != nil {
			return err
		}
		for name, value := range result.Const {
			expanded = strings.ReplaceAll(expanded, "${Unsafe."+name+"}", value)
			expanded = strings.ReplaceAll(expanded, "${"+name+"}", value)
		}
		connector := item.Connector
		if connector == "" {
			connector = source.Connector
		}
		columns, err := column.ResolvePrepared(ctx, column.PreparedRequest{Component: source.Name, View: item.Name, Connector: connector, SQL: expanded, ExecutableSQL: sql}, resolve)
		if err != nil {
			return err
		}
		// An annotation may refine a known result; it cannot create a physical column.
		if item.Declaration != nil {
			for name := range item.Declaration.ColumnsConfig {
				found := false
				for _, col := range columns {
					if strings.EqualFold(name, col.Name) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("prepared columns %s: annotation target %s is absent", item.Name, name)
				}
			}
		}
		item.PreparedColumns = columns
	}
	return nil
}

func expandPreparedSQL(source *shape.Source, sql string, active map[string]bool) (string, error) {
	for i := 0; i < len(sql); i++ {
		if sql[i] != '$' || i+1 >= len(sql) || sql[i+1] != '{' {
			continue
		}
		body, end, ok := readCompileTemplateExpr(sql, i+1)
		if !ok {
			continue
		}
		body = strings.TrimSpace(body)
		if !strings.HasPrefix(strings.ToLower(body), "embed:") {
			i = end
			continue
		}
		ref := strings.Trim(strings.TrimSpace(body[len("embed:"):]), `"'`)
		path := resolveEmbedPath(source, ref)
		if path == "" || active[path] {
			return "", fmt.Errorf("prepared columns: missing or cyclic embedded SQL %q", ref)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("prepared columns: embedded SQL %q: %w", ref, err)
		}
		active[path] = true
		expanded, err := expandPreparedSQL(source, string(data), active)
		delete(active, path)
		if err != nil {
			return "", err
		}
		sql = sql[:i] + expanded + sql[end+1:]
		i += len(expanded) - 1
	}
	return sql, nil
}
