package sql

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

// parseProjectionQuery keeps authored MySQL variable operands in the existing
// parser AST. SQL text and evaluation remain owned by the database; extensions
// must consume one complete operand and may never recover malformed expressions.
func parseProjectionQuery(source string) (*query.Select, error) {
	return sqlparser.ParseQuery(source, sqlparser.WithErrorHandler(projectionVariableOperand))
}

func projectionVariableOperand(err error, cursor *parsly.Cursor, destination any) error {
	operand, ok := destination.(*node.Node)
	if !ok || cursor.Pos >= len(cursor.Input) || cursor.Input[cursor.Pos] != '@' {
		return err
	}
	start := cursor.Pos
	end := start + 1
	for end < len(cursor.Input) {
		b := cursor.Input[end]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '$' || b == '.') {
			break
		}
		end++
	}
	if end == start+1 {
		return err
	}
	*operand = &expr.Ident{Name: string(cursor.Input[start:end])}
	cursor.Pos = end
	return nil
}
