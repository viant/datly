package inference

import (
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/sqlparser"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	"github.com/viant/velty/parser"
	"reflect"
	"strings"
)

type Logf func(format string, a ...interface{}) (n int, err error)

type Template struct {
	SQL          string
	Table        *Table
	State        State
	fragments    []string
	implicitKind view.Kind
	variables    map[string]bool
}

func NewTemplate(SQL string, state State, table *Table, implicitKind view.Kind) *Template {
	return &Template{
		Table:        table,
		SQL:          SQL,
		State:        state,
		implicitKind: implicitKind,
	}
}

func (t *Template) DetectParameters() error {
	if err := t.tryDetectParameters(); err != nil {
		return err
	}
	return nil
}

func (t *Template) isParameter(paramName string) bool {
	if isVariable := t.variables[paramName]; isVariable {
		return false
	}
	return sanitize.CanBeParam(paramName)
}

func (t *Template) tryDetectParameters() error {
	aBlock, err := parser.Parse([]byte(t.SQL))
	if err != nil {
		return err
	}
	t.detectParameters(aBlock.Stmt, true, nil, view.One)
	return nil
}

func (t *Template) detectParameters(statements []ast.Statement, required bool, rType reflect.Type, cardinality view.Cardinality) {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case stmt.ForEach:
			t.variables[actual.Item.ID] = true
		case stmt.Statement:
			t.parseStatementAndAppend(&actual, required, rType, cardinality)
		case *expr.Select:
			t.parseSelectAndAppend(actual, required, rType, cardinality)
			callExpr := actual.X
			for callExpr != nil {
				switch callType := callExpr.(type) {
				case *expr.Select:
					callExpr = callType.X
				case *expr.Call:
					for _, arg := range callType.Args {
						t.detectParameters([]ast.Statement{arg}, required, arg.Type(), view.One)
					}
					callExpr = callType.X
				case *expr.SliceIndex:
					t.detectParameters([]ast.Statement{callType.X}, required, callType.Type(), view.One)
					callExpr = callType.Y
				default:
					callExpr = nil
				}
			}
		case *stmt.Statement:
			selector, ok := asSelector(actual.X)
			if ok {
				t.variables[view.FirstNotEmpty(selector.FullName, selector.ID)] = true
			}

			t.parseStatementAndAppend(actual, required, rType, cardinality)
		case *stmt.ForEach:
			t.variables[actual.Item.ID] = true
			set, ok := actual.Set.(*expr.Select)
			if ok && !t.variables[set.ID] {
				t.detectParameters([]ast.Statement{set}, false, rType, view.Many)
			}

		case *expr.Unary:
			t.detectParameters([]ast.Statement{actual.X}, false, actual.Type(), view.One)
		case *expr.Binary:
			xType := actual.X.Type()
			if xType == nil {
				xType = actual.Y.Type()
			}
			t.detectParameters([]ast.Statement{actual.X, actual.Y}, false, xType, view.One)
		case *expr.Parentheses:
			t.detectParameters([]ast.Statement{actual.P}, false, actual.Type(), view.One)
		case *stmt.If:
			t.detectParameters([]ast.Statement{actual.Condition}, false, actual.Type(), view.One)
			if actual.Else != nil {
				t.detectParameters([]ast.Statement{actual.Else}, false, actual.Else.Type(), view.One)
			}
		case *stmt.Append:
			t.fragments = append(t.fragments, actual.Append)
		}

		switch actual := statement.(type) {
		case ast.StatementContainer:
			t.detectParameters(actual.Statements(), false, nil, cardinality)
		}
	}
}

func (t *Template) parseStatementAndAppend(actual *stmt.Statement, required bool, rType reflect.Type, cardinality view.Cardinality) {
	x, ok := actual.X.(*expr.Select)
	if ok {
		t.variables[x.ID] = true
	}

	y, ok := actual.Y.(*expr.Select)
	if ok && !t.variables[y.ID] {
		t.parseSelectAndAppend(y, required, rType, cardinality)
	}
}

func (t *Template) parseSelectAndAppend(actual *expr.Select, required bool, rType reflect.Type, cardinality view.Cardinality) {
	var prefix, paramName string
	if actual.X != nil {
		if _, ok := actual.X.(*expr.Call); ok {
			paramName = actual.ID
		}
	}
	if paramName == "" {
		prefix, paramName = sanitize.GetHolderName(actual.FullName)
	}
	if !t.isParameter(paramName) {
		return
	}
	if prefix != "" && t.State.Has(prefix) { //parameter already defined
		return
	}
	parameter := t.State.Lookup(paramName)
	if parameter != nil && parameter.HasDataType() { //parameter already defined
		return
	}
	selector, ok := getContextSelector(prefix, actual.X)
	if ok && selector.ID == "IndexBy" {
		cardinality = view.Many
	}
	if parameter == nil {
		parameter = &Parameter{Parameter: view.Parameter{Name: paramName, In: &view.Location{Kind: t.implicitKind, Name: paramName}}}
		parameter.EnsureSchema()
		parameter.Schema.Cardinality = cardinality
		if rType != nil && prefix != keywords.ParamsMetadataKey {
			parameter.Schema.DataType = rType.String()
		}
		parameter.Required = &required
	}
	operator, column := t.detectExprContext()
	if column != nil {
		parameter.Schema.DataType = column.Type
	}
	if operator == "in" {
		parameter.Schema.Cardinality = view.Many
		//TODO add condec asStrings, or asInts
	}
	t.State.Append(parameter)
}

func (t *Template) detectExprContext() (string, *sqlparser.Column) {
	if len(t.fragments) == 0 {
		return "", nil
	}
	last := t.fragments[len(t.fragments)-1]
	elements := SplitByWhitespace(last)
	if len(elements) <= 1 {
		return "", nil
	}
	operator := ""
	var column *sqlparser.Column
	operatorIndex := -1
	for i := len(elements) - 1; i >= 0; i-- {
		candidate := strings.ToLower(elements[i])
		switch candidate {
		case "=", ">=", "<=", "!=", "in":
			operator = candidate
			operatorIndex = i
			break
		}
	}
	for i := operatorIndex - 1; i >= 0; i-- {
		candidate := strings.ToLower(elements[i])
		if column = t.Table.Lookup(candidate); column != nil {
			break
		}
	}
	if column == nil {
		operator = ""
	}
	return operator, column
}

func getContextSelector(prefix string, x ast.Expression) (*expr.Select, bool) {
	selector, ok := asSelector(x)
	if prefix == "" || !ok {
		return selector, ok
	}
	return asSelector(selector.X)
}

func asSelector(x ast.Expression) (*expr.Select, bool) {
	selector, ok := x.(*expr.Select)
	return selector, ok
}