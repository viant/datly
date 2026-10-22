package dml

import (
	"strings"

	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/source"
	"github.com/viant/sqlparser/update"
	"github.com/viant/sqlx/metadata/info"
)

// Recognizing an unrelated, side-effect-free UPDATE preserves an earlier
// buffered parent without changing or draining the original bulk statement.
// This is proof eligibility only: all unrecognized SQL remains an opaque
// barrier and executes through the same ordinary Execute path as before.
func distinctBufferedUpdateSQL(parent, text string, dialect *info.Dialect) bool {
	if dialect == nil || source.ValidateStructure(text) != nil || strings.ContainsAny(text, ";#$") ||
		strings.Contains(text, "/*") || strings.Contains(text, "--") {
		return false
	}
	cursor := parsly.NewCursor("", []byte(text), 0)
	statement := &update.Statement{}
	if sqlparser.Parse(cursor, statement) != nil || strings.TrimSpace(text[cursor.Pos:]) != "" || len(statement.Set) == 0 {
		return false
	}
	target, ok := bufferedUpdateIdentifier(statement.Target.X, dialect)
	if !ok || !distinctBufferedDeleteTarget(parent, target, dialect) {
		return false
	}
	for _, assignment := range statement.Set {
		if assignment == nil {
			return false
		}
		column, ok := assignment.Column.(*expr.Ident)
		if !ok || !bufferedUpdateSimpleName(column, dialect) || !bufferedUpdatePlaceholder(assignment.Expr) {
			return false
		}
	}
	return statement.Qualify != nil && bufferedUpdatePredicate(statement.Qualify.X, dialect)
}

func bufferedUpdateIdentifier(n node.Node, dialect *info.Dialect) (string, bool) {
	var text string
	switch value := n.(type) {
	case *expr.Ident:
		text = value.Name
	case *expr.Selector:
		if value.Expression != "" {
			return "", false
		}
		tail, ok := bufferedUpdateIdentifier(value.X, dialect)
		if !ok {
			return "", false
		}
		text = value.Name + "." + tail
	default:
		return "", false
	}
	_, err := (sqlparser.TableIdentifierParser{Product: dialect.Name}).Parts(text)
	return text, err == nil
}

func bufferedUpdateSimpleName(n node.Node, dialect *info.Dialect) bool {
	name, ok := n.(*expr.Ident)
	if !ok {
		return false
	}
	parts, err := (sqlparser.TableIdentifierParser{Product: dialect.Name}).Parts(name.Name)
	return err == nil && len(parts) == 1
}

func bufferedUpdatePlaceholder(n node.Node) bool {
	value, ok := n.(*expr.Placeholder)
	return ok && value.Name == "?" && value.Comments == ""
}

func bufferedUpdatePredicate(n node.Node, dialect *info.Dialect) bool {
	binary, ok := n.(*expr.Binary)
	if !ok || !bufferedUpdateSimpleName(binary.X, dialect) {
		return false
	}
	switch strings.ToUpper(binary.Op) {
	case "=", "<>", "!=", "<", "<=", ">", ">=":
		return bufferedUpdatePlaceholder(binary.Y)
	case "IN":
		group, ok := binary.Y.(*expr.Parenthesis)
		if !ok {
			return false
		}
		// The parser stores a singleton IN list as its item, without a slice.
		if bufferedUpdatePlaceholder(group.X) {
			return true
		}
		items, ok := group.X.([]node.Node)
		if !ok || len(items) == 0 {
			return false
		}
		for _, item := range items {
			if !bufferedUpdatePlaceholder(item) {
				return false
			}
		}
		return true
	}
	return false
}
