package column

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/sink"
)

type tableConstraint struct {
	primaryKey    bool
	autoIncrement bool
	unique        bool
	notNull       bool
	defaultValue  *string
}

type projectionLineage struct {
	direct   map[string]string
	blocked  map[string]bool
	wildcard bool
}

func loadTableConstraints(ctx context.Context, db *sql.DB, table string) (map[string]tableConstraint, error) {
	session, err := config.Session(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("load SQLX metadata session: %w", err)
	}
	columns, err := config.Columns(ctx, session, db, table)
	if err != nil {
		return nil, fmt.Errorf("load SQLX table metadata for %q: %w", table, err)
	}
	return constraintsFromColumns(columns), nil
}

func constraintsFromColumns(columns []sink.Column) map[string]tableConstraint {
	result := make(map[string]tableConstraint, len(columns))
	for index := range columns {
		column := &columns[index]
		name := normalizedName(column.Name)
		if name == "" {
			continue
		}
		primaryKey := strings.EqualFold(strings.TrimSpace(column.Key), "PRI")
		constraint := tableConstraint{
			primaryKey:    primaryKey,
			autoIncrement: column.Autoincrement(),
			unique:        !primaryKey && column.IsUnique(),
			notNull:       column.IsNotNull(),
		}
		if column.Default != nil && strings.TrimSpace(*column.Default) != "" {
			value := *column.Default
			constraint.defaultValue = &value
		}
		result[name] = constraint
	}
	return result
}

func directProjectionLineage(source *spec.ViewSource) (*projectionLineage, error) {
	result := &projectionLineage{direct: map[string]string{}, blocked: map[string]bool{}}
	if source == nil || strings.TrimSpace(source.Table) == "" {
		return result, nil
	}
	text := strings.TrimSpace(source.SQL)
	if text == "" {
		result.wildcard = true
		return result, nil
	}
	parsed, err := sqlparser.ParseQuery(text)
	if err != nil {
		return nil, fmt.Errorf("parse table projection lineage: %w", err)
	}
	if parsed == nil || parsed.Union != nil || !sameTable(sqlparser.TableName(parsed), source.Table) {
		return result, nil
	}
	allowedNamespaces := map[string]bool{}
	rootNamespaces := []string{parsed.From.Alias}
	if strings.TrimSpace(parsed.From.Alias) == "" {
		rootNamespaces = []string{source.Table, tableBase(source.Table)}
	}
	for _, namespace := range rootNamespaces {
		if namespace = normalizedName(namespace); namespace != "" {
			allowedNamespaces[namespace] = true
		}
	}
	requireQualifier := len(parsed.Joins) > 0
	for _, item := range parsed.List {
		if item == nil {
			continue
		}
		namespace, wildcard := projectionWildcard(item.Expr)
		if wildcard && lineageNamespaceAllowed(namespace, allowedNamespaces, requireQualifier) {
			result.wildcard = true
		}
	}
	for _, column := range sqlparser.NewColumns(parsed.List) {
		if column == nil {
			continue
		}
		output := normalizedName(column.Identity())
		if output == "" {
			continue
		}
		if !lineageNamespaceAllowed(column.Namespace, allowedNamespaces, requireQualifier) {
			result.blocked[output] = true
			continue
		}
		if strings.TrimSpace(column.Name) == "*" {
			result.wildcard = true
			continue
		}
		if strings.TrimSpace(column.Expression) != "" {
			result.blocked[output] = true
			continue
		}
		sourceName := normalizedName(column.Name)
		if sourceName == "" {
			result.blocked[output] = true
			continue
		}
		result.direct[output] = sourceName
	}
	return result, nil
}

func projectionWildcard(projection node.Node) (string, bool) {
	switch actual := projection.(type) {
	case *expr.Star:
		return "", true
	case *expr.Selector:
		_, wildcard := actual.X.(*expr.Star)
		return actual.Name, wildcard
	default:
		return "", false
	}
}

func applyTableConstraints(columns []*spec.Column, constraints map[string]tableConstraint, lineage *projectionLineage) {
	if len(columns) == 0 || len(constraints) == 0 || lineage == nil {
		return
	}
	for _, column := range columns {
		if column == nil {
			continue
		}
		output := normalizedName(column.Name)
		sourceName, ok := lineage.direct[output]
		if !ok && lineage.wildcard && !lineage.blocked[output] {
			sourceName = output
			ok = sourceName != ""
		}
		if !ok {
			continue
		}
		constraint, ok := constraints[sourceName]
		if !ok {
			continue
		}
		column.PrimaryKey = column.PrimaryKey || constraint.primaryKey
		column.AutoIncrement = column.AutoIncrement || constraint.autoIncrement
		column.Unique = column.Unique || constraint.unique
		column.NotNull = column.NotNull || constraint.notNull
		if column.Default == nil && constraint.defaultValue != nil {
			value := *constraint.defaultValue
			column.Default = &value
		}
	}
}

func lineageNamespaceAllowed(namespace string, allowed map[string]bool, requireQualifier bool) bool {
	namespace = normalizedName(namespace)
	if namespace == "" {
		return !requireQualifier
	}
	return allowed[namespace]
}

func sameTable(actual, expected string) bool {
	actual = normalizedTable(actual)
	expected = normalizedTable(expected)
	return actual != "" && actual == expected
}

func normalizedTable(value string) string {
	value = strings.TrimSpace(value)
	parts := strings.Split(value, ".")
	for index, part := range parts {
		parts[index] = strings.Trim(strings.TrimSpace(part), "`\"[]")
	}
	return strings.ToLower(strings.Join(parts, "."))
}

func tableBase(value string) string {
	parts := strings.Split(normalizedTable(value), ".")
	return parts[len(parts)-1]
}

func normalizedName(value string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(value), "`\"[]"))
}
