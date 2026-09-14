package dql

import (
	"fmt"
	"strings"

	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	qexpr "github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

type DeclarationAnalysis struct {
	SQL                       string
	FromPath                  string
	Table                     string
	Projection                []DeclarationProjection
	ProjectionComplete        bool
	DeclarationCriteriaIn     *DeclarationCriteriaIn
	DeclarationScalarProperty *DeclarationScalarProperty
	DataType                  string
}

// DeclarationProjection is one parser-proven StructQL destination field.
// Name is the result alias (or source name when no alias was authored), while
// Source is the selected source field used to derive its concrete Go type.
type DeclarationProjection struct {
	Name      string
	Source    string
	Aggregate bool
}

type DeclarationCriteriaIn struct {
	Column      string
	HelperField string
}

type DeclarationScalarProperty struct {
	Column      string
	SourceField string
	Property    string
}

func AnalyzeDeclarationSQL(raw string) (*DeclarationAnalysis, error) {
	hint, sqlText, err := splitHintAndSQL(raw)
	if err != nil {
		return nil, err
	}
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return nil, nil
	}
	parsed, err := sqlparser.ParseQuery(sqlText, withDeclarationExpressions())
	if err != nil {
		return nil, fmt.Errorf("parse declaration sql %q: %w", sqlText, err)
	}
	if parsed == nil {
		return nil, fmt.Errorf("parse declaration sql %q: no query parsed", sqlText)
	}
	result := &DeclarationAnalysis{
		SQL:      sqlText,
		FromPath: strings.TrimSpace(parsed.From.Unparsed),
		Table:    fromTable(parsed.From.X),
		DataType: normalizeDataType(firstNonEmpty(decodeDataTypeHint(hint), decodeProjectionDataType(parsed.List))),
	}
	result.Projection, result.ProjectionComplete = parseDeclarationProjection(parsed.List)
	result.DeclarationCriteriaIn = parseCriteriaIn(parsed.Qualify)
	result.DeclarationScalarProperty = parseScalarProperty(parsed.Qualify)
	return result, nil
}

func (a *DeclarationAnalysis) NestedChildField() string {
	if a == nil {
		return ""
	}
	path := strings.TrimSpace(a.FromPath)
	if !strings.HasPrefix(path, "/") {
		return ""
	}
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return ""
	}
	return path
}

func withDeclarationExpressions() sqlparser.Option {
	return sqlparser.WithErrorHandler(func(err error, cur *parsly.Cursor, destNode interface{}) error {
		fromNode, ok := destNode.(*query.From)
		if !ok {
			return err
		}
		if cur.Pos >= len(cur.Input) || cur.Input[cur.Pos] != '/' {
			return err
		}
		start := cur.Pos
		cur.Pos++
		for cur.Pos < len(cur.Input) {
			ch := cur.Input[cur.Pos]
			if ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r' {
				break
			}
			cur.Pos++
		}
		fromNode.Unparsed = string(cur.Input[start:cur.Pos])
		return nil
	})
}

func parseDeclarationProjection(list query.List) ([]DeclarationProjection, bool) {
	columns := sqlparser.NewColumns(list)
	if columns.IsStarExpr() {
		return nil, false
	}
	result := make([]DeclarationProjection, 0, len(list))
	for _, item := range list {
		if item == nil {
			continue
		}
		var source string
		aggregate := false
		switch actual := item.Expr.(type) {
		case *qexpr.Ident:
			source = actual.Name
		case *qexpr.Selector:
			source = trimSelectorSuffix(sqlparser.Stringify(actual))
		case *qexpr.Call:
			if len(list) != 1 || !strings.EqualFold(sqlparser.Stringify(actual.X), "ARRAY_AGG") || len(actual.Args) != 1 {
				return nil, false
			}
			source = trimSelectorSuffix(sqlparser.Stringify(actual.Args[0]))
			aggregate = true
		default:
			return nil, false
		}
		name := strings.TrimSpace(item.Alias)
		if name == "" {
			name = source
		}
		result = append(result, DeclarationProjection{Name: name, Source: source, Aggregate: aggregate})
	}
	return result, len(result) > 0
}

func fromTable(from node.Node) string {
	switch actual := from.(type) {
	case *qexpr.Ident:
		return actual.Name
	case *qexpr.Selector:
		return sqlparser.Stringify(actual)
	default:
		return ""
	}
}

func trimSelectorSuffix(value string) string {
	value = strings.TrimSpace(value)
	if i := strings.LastIndex(value, "."); i != -1 && i+1 < len(value) {
		return strings.TrimSpace(value[i+1:])
	}
	return value
}
