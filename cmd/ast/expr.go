package ast

import (
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
	"github.com/viant/sqlx/metadata/ast/parser"
	"strings"
)

type Criterion struct {
	X, Y, Op string
}

func ExtractCriteriaPlaceholders(node node.Node, list *[]*Criterion) {
	if node == nil {
		return
	}
	switch actual := node.(type) {
	case *expr.Binary:
		op := strings.ToUpper(actual.Op)
		switch op {
		case "AND", "OR":
			ExtractCriteriaPlaceholders(actual.X, list)
			ExtractCriteriaPlaceholders(actual.Y, list)
		default:
			if bin, ok := actual.Y.(*expr.Binary); ok {
				ExtractCriteriaPlaceholders(bin.Y, list)
				switch operand := actual.X.(type) {
				case *expr.Selector, *expr.Ident, *expr.Placeholder:
					appendParamExpr(operand, actual.Op, bin.X, list)
				}
				return
			}
			appendParamExpr(actual.X, actual.Op, actual.Y, list)
		}
	case *expr.Unary:

	}
}

func appendParamExpr(x node.Node, op string, y node.Node, list *[]*Criterion) {
	*list = append(*list, &Criterion{Op: op, X: parser.Stringify(x), Y: parser.Stringify(y)})
}
