package dql

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	veltyparser "github.com/viant/velty/parser"
)

// TemplateExpressions hands template operands to Velty while SQLParser owns
// the surrounding SQL. Raw nodes retain the authored bytes for later execution.
type TemplateExpressions struct{}

func (TemplateExpressions) Parse(err error, cursor *parsly.Cursor, destination any) error {
	operand, ok := destination.(*node.Node)
	if !ok || cursor == nil || cursor.Pos >= len(cursor.Input) || cursor.Input[cursor.Pos] != '#' {
		return err
	}
	start := cursor.Pos
	if _, templateErr := veltyparser.MatchStatement(cursor); templateErr != nil {
		return templateErr
	}
	*operand = expr.NewRaw(string(cursor.Input[start:cursor.Pos]))
	return nil
}
