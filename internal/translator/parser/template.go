package parser

import (
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	"github.com/viant/velty/parser"
	"reflect"
	"strings"
)

type (
	Template struct {
		SQL       string
		Declared  map[string]bool
		State     *inference.State
		Context   []*ExpressionContext
		fragments []string
	}

	ExpressionContext struct {
		Name      string
		Context   Context
		Function  string
		Fragments []string
		Column    string
		Type      reflect.Type
	}
)

func (c *ExpressionContext) BeforeElements() []string {
	if len(c.Fragments) == 0 {
		return []string{}
	}
	last := c.Fragments[len(c.Fragments)-1]
	return SplitByWhitespace(last)
}

func (c *ExpressionContext) IsJSONCodec() bool {
	if len(c.Fragments) == 0 {
		return false
	}
	last := c.Fragments[len(c.Fragments)-1]
	return strings.Contains(last, `"Codec"`) && strings.Contains(last, `"JSON"`)
}

func (t *Template) initContext(context Context, fnName string, statements ...ast.Statement) {
outer:
	for _, statement := range statements {
		if statement == nil {
			continue
		}
		switch actual := statement.(type) {
		case *expr.Select:
			paramContext := NewParamContext(shared.FirstNotEmpty(actual.FullName, actual.ID), fnName, context)
			if len(t.fragments) > 0 {
				paramContext.Fragments = t.fragments
				t.fragments = []string{}
			}
			t.Context = append(t.Context, paramContext)
			currentSelector := actual
			for actual.X != nil {
				xSelect, ok := actual.X.(*expr.Select)
				if ok {
					actual = xSelect
					continue
				}
				asFunc, ok := actual.X.(*expr.Call)
				if ok {
					for _, arg := range asFunc.Args {
						t.initContext(FuncContext, currentSelector.ID+"."+actual.ID, arg)
					}
				}
				asSlice, ok := actual.X.(*expr.SliceIndex)
				if ok {
					t.initContext(context, fnName, asSlice.X, asSlice.Y)
				}

				if asFunc != nil && asFunc.X != nil {
					xSelect, ok = asFunc.X.(*expr.Select)
					if ok {
						actual = xSelect
						continue
					}
				}
				continue outer
			}

		case *expr.Parentheses:
			t.initContext(context, fnName, actual.P)
		case *expr.Unary:
			t.initContext(context, fnName, actual.X)
		case *expr.Binary:
			t.initContext(context, fnName, actual.X, actual.Y)
		case *stmt.ForEach:
			t.addVariable(actual.Item)
			t.initContext(ForEachContext, "", actual.Item, actual.Set)
			t.initContext(AppendContext, "", actual.Body.Stmt...)
		case *stmt.If:
			t.initContext(IfContext, "", actual.Condition)
			t.initContext(AppendContext, "", actual.Body.Stmt...)
			if actual.Else != nil {
				t.initContext(IfContext, "", actual.Else)
			}
		case *stmt.Statement:
			selector, ok := actual.X.(*expr.Select)
			if ok {
				t.addVariable(selector)
			}
			t.addVariable(selector)
			t.initContext(SetContext, "", actual.X, actual.Y)
		case *stmt.Append:
			t.fragments = append(t.fragments, actual.Append)
		}
	}
}

func (t *Template) addVariable(selector *expr.Select) {
	_, holderName := GetHolderName(shared.FirstNotEmpty(selector.FullName, selector.ID))
	if keywords.Has(holderName) {
		return
	}
	if selector.X != nil { //variable can be only top level
		return
	}
	t.Declared[holderName] = true
}

func NewTemplate(SQL string, state *inference.State) (*Template, error) {
	block, err := parser.Parse([]byte(SQL))
	if err != nil {
		return nil, err
	}
	ret := &Template{SQL: SQL, Declared: map[string]bool{}, State: state}
	ret.initContext(AppendContext, "", block.Statements()...)
	return ret, nil
}

func NewParamContext(name string, fnName string, context Context) *ExpressionContext {
	return &ExpressionContext{
		Name:     name,
		Context:  context,
		Function: fnName,
	}
}
