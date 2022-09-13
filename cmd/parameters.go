package cmd

import (
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/sanitizer"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
	"github.com/viant/sqlx/metadata/ast/parser"
	"strings"
)

type parameter struct {
	param, column, selector string
}

type parameters []*parameter

func (p *parameters) nextColumn() *parameter {
	if len(*p) == 0 {
		*p = append(*p, &parameter{})
		return (*p)[0]
	}
	for i, item := range *p {
		if item.column == "" {
			return (*p)[i]
		}
	}
	ret := &parameter{}
	*p = append(*p, ret)
	return ret

}

func (p *parameters) nextParam() *parameter {
	if len(*p) == 0 {
		*p = append(*p, &parameter{})
		return (*p)[0]
	}
	for i, item := range *p {
		if item.param == "" {
			return (*p)[i]
		}
	}
	ret := &parameter{}
	*p = append(*p, ret)
	return ret
}

func (s *serverBuilder) updateParameterTypes(table *option.Table) {
	if table.ViewMeta == nil {
		return
	}

	for _, param := range table.ViewMeta.Parameters {
		if !param.Assumed {
			continue
		}

		if param.Typer == nil {
			continue
		}

		switch actual := param.Typer.(type) {
		case *sanitizer.ColumnType:
			aType := table.ColumnTypes[actual.ColumnName]
			if aType == "" {
				dotIndex := strings.Index(actual.ColumnName, ".")
				if dotIndex != -1 {
					aType = table.ColumnTypes[actual.ColumnName[dotIndex+1:]]
				}
			}

			if aType != "" {
				param.DataType = aType
			}

		case *sanitizer.LiteralType:
			param.DataType = actual.RType.String()
		}
	}
}

func discoverParameterColumn(x node.Node, list *parameters) {
	switch n := x.(type) {
	case *expr.Binary:
		discoverOperand(n.X, list)
		discoverOperand(n.Y, list)
	case *expr.Unary:
		discoverParameterColumn(n.X, list)
	case *expr.Parenthesis:
		cursor := parsly.NewCursor("", []byte(strings.Trim(n.Raw, "()")), 0)
		qualify := &expr.Qualify{}
		if err := parser.ParseQualify(cursor, qualify); err == nil {
			discoverParameterColumn(qualify.X, list)
		}
	}
}

func discoverOperand(n node.Node, list *parameters) {
	switch x := n.(type) {
	case *expr.Placeholder:
		pair := list.nextParam()
		pair.param = x.Name
	case *expr.Selector:
		pair := list.nextColumn()
		pair.selector = parser.Stringify(x)
		pair.column = parser.Stringify(x.X)
	case *expr.Ident:
		pair := list.nextColumn()
		pair.column = x.Name
	default:
		discoverParameterColumn(x, list)
	}
}

func extractDataViewParams(params []*option.Parameter, routeOption *option.Route, hints sanitizer.ParameterHints) (map[string]*option.TableParam, error) {
	result := map[string]*option.TableParam{}
	for _, param := range params {
		if param.Kind != string(view.DataViewKind) {
			continue
		}

		xTable, _, err := ParseSQLx(param.SQL, routeOption, hints)
		if err != nil {
			return nil, err
		}

		result[param.Name] = &option.TableParam{
			Param: convertMetaParameter(param),
			Table: xTable,
		}
	}

	return result, nil
}
