// Package locking owns dialect checks and physical-source row-lock placement.
package locking

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	sqlsource "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx/metadata/info"
)

var tablePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*$`)
var aliasPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

func Target(value string) (table, alias string, err error) {
	parts := strings.Fields(value)
	if len(parts) < 1 || len(parts) > 2 || !tablePattern.MatchString(parts[0]) {
		return "", "", fmt.Errorf("row lock target requires an explicit physical table and optional alias")
	}
	table = parts[0]
	if len(parts) == 2 {
		alias = parts[1]
		if !aliasPattern.MatchString(alias) {
			return "", "", fmt.Errorf("invalid row lock alias %q", alias)
		}
	}
	return
}

// Clause does not start or complete a transaction. SQLite uses transaction locks.
func Clause(dialect *info.Dialect, active bool) (string, error) {
	if !active {
		return "", fmt.Errorf("FOR UPDATE requires an active transaction")
	}
	if dialect == nil {
		return "", fmt.Errorf("FOR UPDATE requires a dialect")
	}
	switch strings.ToLower(strings.TrimSpace(dialect.Name)) {
	case "mysql", "postgresql", "postgres", "pg":
		return "FOR UPDATE", nil
	case "sqlite", "sqlite3":
		return "", nil
	default:
		return "", fmt.Errorf("FOR UPDATE is unsupported for dialect %q", dialect.Name)
	}
}

// Apply locates the unique SELECT directly reading the declared physical target.
// It preserves bindings and inserts inside derived sources rather than locking a
// wrapper that cannot protect its underlying rows on MySQL.
func Apply(source, target string, dialect *info.Dialect, active bool) (string, error) {
	return ApplyOrdered(source, target, "", dialect, active)
}

func ApplyOrdered(source, target, order string, dialect *info.Dialect, active bool) (string, error) {
	table, alias, err := Target(target)
	if err != nil {
		return "", err
	}
	clause, err := Clause(dialect, active)
	if err != nil {
		return "", err
	}
	if err := OrderTarget(target, order); err != nil {
		return "", err
	}
	out, count, err := rewrite(source, table, alias, order, clause)
	if err != nil {
		return "", err
	}
	if count != 1 {
		return "", fmt.Errorf("row lock target %q matched %d physical SELECT sources", target, count)
	}
	return out, nil
}

func rewrite(source, table, alias, order, clause string) (string, int, error) {
	matches := 0
	trimmed := strings.TrimSpace(source)
	if strings.HasPrefix(strings.ToUpper(trimmed), "SELECT") {
		parsed, err := sqlparser.ParseQuery(trimmed)
		if err != nil {
			return "", 0, fmt.Errorf("parse row lock source: %w", err)
		}
		physical := physicalTable(parsed.From.X)
		if strings.EqualFold(physical, table) && (alias == "" || strings.EqualFold(parsed.From.Alias, alias)) {
			if parsed.Union != nil || len(parsed.GroupBy) > 0 || parsed.Having != nil || aggregateProjection(parsed.List) {
				return "", 0, fmt.Errorf("row locks require physical rows, not an aggregate or union")
			}
			if sqlsource.Token("FOR UPDATE").Contains(trimmed) {
				return "", 0, fmt.Errorf("row lock source already contains FOR UPDATE")
			}
			matches++
		}
	}
	if matches == 1 {
		return finish(source, order, clause), 1, nil
	}
	scanner := sqlsource.NewCodeScanner(source, 0)
	skipUntil := 0
	cursor := 0
	var out strings.Builder
	for pos, ok := scanner.Next(); ok; pos, ok = scanner.Next() {
		if pos < skipUntil || source[pos] != '(' {
			continue
		}
		group, end, ok := sqlsource.ReadGroupString(source, pos, '(', ')')
		if !ok {
			return "", 0, fmt.Errorf("invalid row lock SQL group")
		}
		skipUntil = end
		inner := group[1 : len(group)-1]
		changed, count, err := rewrite(inner, table, alias, order, clause)
		if err != nil {
			return "", 0, err
		}
		matches += count
		out.WriteString(source[cursor : pos+1])
		out.WriteString(changed)
		out.WriteByte(')')
		cursor = end
	}
	out.WriteString(source[cursor:])
	result := out.String()

	return result, matches, nil
}
func physicalTable(n node.Node) string {
	switch n.(type) {
	case *expr.Ident, *expr.Selector:
		table, _, err := sqlparser.SourceTable(n)
		if err == nil {
			return table
		}
	}
	return ""
}
func aggregateProjection(list query.List) bool {
	for _, item := range list {
		if call, ok := item.Expr.(*expr.Call); ok {
			switch strings.ToLower(sqlparser.Stringify(call.X)) {
			case "count", "sum", "avg", "min", "max", "group_concat", "json_agg", "json_arrayagg":
				return true
			}
		}
	}
	return false
}

func OrderTarget(target, order string) error {
	if order == "" {
		return nil
	}
	table, alias, err := Target(target)
	if err != nil {
		return err
	}
	parts := strings.Split(order, ".")
	if len(parts) != 2 || !aliasPattern.MatchString(parts[0]) || !aliasPattern.MatchString(parts[1]) {
		return fmt.Errorf("row lock order requires a qualified physical column")
	}
	if alias == "" {
		segments := strings.Split(table, ".")
		alias = segments[len(segments)-1]
	}
	if !strings.EqualFold(parts[0], alias) {
		return fmt.Errorf("row lock order must use physical target alias %q", alias)
	}
	return nil
}
func prependOrder(source, order string) string {
	if pos := sqlsource.FindTopLevelKeyword(source, "ORDER BY", 0); pos >= 0 {
		by := sqlsource.FindTopLevelKeyword(source, "BY", pos+len("ORDER"))
		return source[:by+2] + " " + order + " ASC," + source[by+2:]
	}
	boundary := sqlsource.CriteriaBoundary(source)
	return source[:boundary] + " ORDER BY " + order + " ASC " + source[boundary:]
}

// Once a block directly owns the target, nested scalar/EXISTS subqueries use
// their own scopes and do not make the physical source ambiguous.
func finish(source, order, clause string) string {
	result := strings.TrimSpace(source)
	suffix := ""
	if strings.HasSuffix(result, ";") {
		result = strings.TrimSuffix(result, ";")
		suffix = ";"
	}
	if order != "" {
		result = prependOrder(result, order)
	}
	if clause != "" {
		result += "\n" + clause
	}
	return result + suffix
}
