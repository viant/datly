package sql

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"strings"
)

// Projection keeps source-scope SQL separate from an optional result wrapper.
// Builders complete predicates/bindings in Source before rendering the result.
type Projection struct {
	Source  string
	outer   []string
	columns []ProjectionColumn
}

func (p *Projection) Render(source string) string {
	if len(p.outer) == 0 {
		return source
	}
	return "SELECT " + strings.Join(p.outer, ", ") + " FROM (" + strings.TrimSuffix(strings.TrimSpace(source), ";") + ") AS datly_view"
}

// OutputControls resolves ordering through the actual source/output mapping
// when a result wrapper changes scope. A matching terminal spelling alone is
// never enough to authorize another source namespace.
func (p *Projection) OutputControls(controls *spec.ViewControls) (*spec.ViewControls, error) {
	if controls == nil || len(p.outer) == 0 || strings.TrimSpace(controls.OrderBy) == "" {
		return controls, nil
	}
	parsed, err := sqlparser.ParseQuery("SELECT 1 FROM projection_order ORDER BY " + controls.OrderBy)
	if err != nil || parsed == nil || len(parsed.OrderBy) == 0 || parsed.Limit != nil || parsed.Offset != nil || parsed.Union != nil {
		return nil, fmt.Errorf("order by cannot be resolved in projected output scope")
	}
	for _, item := range parsed.OrderBy {
		item.Expr, err = p.outputOrderExpression(item.Expr)
		if err != nil {
			return nil, err
		}
	}
	result := controls.Clone()
	result.OrderBy = sqlparser.Stringify(parsed.OrderBy)
	return result, nil
}

func (p *Projection) outputOrderExpression(value node.Node) (node.Node, error) {
	switch actual := value.(type) {
	case *expr.Literal:
		return actual, nil
	case *expr.Ident, *expr.Selector:
		name := strings.TrimSpace(sqlparser.Stringify(value))
		output := ""
		for _, column := range p.columns {
			if !column.Matches(name) && !(ProjectionNames{column.order}).Matches(name) {
				continue
			}
			if output != "" {
				return nil, fmt.Errorf("ambiguous projected order field %q", name)
			}
			output = column.output
		}
		if output == "" {
			return nil, fmt.Errorf("order by %q is not a declared projected output", name)
		}
		return &expr.Ident{Name: output}, nil
	case *expr.Call:
		copied := *actual
		copied.Args = make([]node.Node, len(actual.Args))
		arguments := make([]string, len(actual.Args))
		for i, argument := range actual.Args {
			resolved, err := p.outputOrderExpression(argument)
			if err != nil {
				return nil, err
			}
			copied.Args[i] = resolved
			arguments[i] = sqlparser.Stringify(resolved)
		}
		copied.Raw = "(" + strings.Join(arguments, ", ") + ")"
		return &copied, nil
	case *expr.Binary:
		copied := *actual
		var err error
		copied.X, err = p.outputOrderExpression(actual.X)
		if err != nil {
			return nil, err
		}
		copied.Y, err = p.outputOrderExpression(actual.Y)
		return &copied, err
	case *expr.Unary:
		copied := *actual
		var err error
		copied.X, err = p.outputOrderExpression(actual.X)
		return &copied, err
	case *expr.Parenthesis:
		copied := *actual
		var err error
		copied.X, err = p.outputOrderExpression(actual.X)
		if err != nil {
			return nil, err
		}
		copied.Raw = "(" + sqlparser.Stringify(copied.X) + ")"
		return &copied, nil
	case *expr.Collate:
		copied := *actual
		var err error
		copied.X, err = p.outputOrderExpression(actual.X)
		return &copied, err
	}
	return nil, fmt.Errorf("order expression %q cannot be resolved in projected output scope", sqlparser.Stringify(value))
}
