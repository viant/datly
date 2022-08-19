package sanitizer

import (
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	"github.com/viant/velty/parser"
)

const (
	UnspecifiedContext Context = iota
	SetContext
	IfContext
	ForEachContext
	AppendContext
)

type (
	Context int

	ParamMetaIterator struct {
		SQL    string
		cursor *parsly.Cursor

		paramMeta      *ParamMeta
		contexts       []*ParamContext
		counter        int
		variables      map[string]bool
		occurrences    map[string]int
		paramMetaTypes map[string]*ParamMetaType
		hints          map[string]*option.ParameterHint
		paramMatcher   *ParamMatcher
	}

	ParamContext struct {
		ParamName string
		Context   Context
	}

	ParamMeta struct {
		Start           int
		End             int
		FullName        string
		Prefix          string
		Holder          string
		Context         Context
		IsVariable      bool
		OccurrenceIndex int
		MetaType        *ParamMetaType
	}

	ParamMetaType struct {
		Typer []option.Typer
		SQL   []string
		Hint  []string
	}
)

func NewIterator(SQL string, hints option.ParameterHints) *ParamMetaIterator {
	result := &ParamMetaIterator{
		SQL:            SQL,
		variables:      map[string]bool{},
		occurrences:    map[string]int{},
		paramMetaTypes: map[string]*ParamMetaType{},
		paramMatcher:   NewParamMatcher(),
		hints:          hints.Index(),
	}

	result.init()

	return result
}

func NewParamContext(name string, context Context) *ParamContext {
	return &ParamContext{
		ParamName: name,
		Context:   context,
	}
}

func (i *ParamMetaIterator) buildContexts(context Context, statements ...ast.Statement) {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case *expr.Select:
			i.contexts = append(i.contexts, NewParamContext(view.NotEmptyOf(actual.FullName, actual.ID), context))
		case *expr.Parentheses:
			i.buildContexts(context, actual.P)
		case *expr.Unary:
			i.buildContexts(context, actual.X)
		case *expr.Binary:
			i.buildContexts(context, actual.X, actual.Y)
		case *stmt.ForEach:
			i.addVariable(actual.Item)
			i.buildContexts(ForEachContext, actual.Item, actual.Set)
			i.buildContexts(AppendContext, actual.Body.Stmt...)
		case *stmt.If:
			i.buildContexts(IfContext, actual.Condition)
			i.buildContexts(AppendContext, actual.Body.Stmt...)
			if actual.Else != nil {
				i.buildContexts(IfContext, actual.Else)
			}
		case *stmt.Statement:
			selector, ok := actual.X.(*expr.Select)
			if ok {
				i.addVariable(selector)
			}

			i.addVariable(selector)
			i.buildContexts(SetContext, actual.X, actual.Y)
		}
	}
}

func (i *ParamMetaIterator) Has() bool {
	if i.cursor.Pos >= i.cursor.InputSize && i.paramMeta == nil {
		return false
	}

	for i.cursor.Pos < i.cursor.InputSize {
		param, pos := i.paramMatcher.TryMatchParam(i.cursor)
		if pos == -1 {
			i.cursor.Pos++
			continue
		}

		i.buildMetaParam(pos, param)
		return true
	}

	return false
}

func (i *ParamMetaIterator) Next() *ParamMeta {
	meta := i.paramMeta
	i.paramMeta = nil

	return meta
}

func (i *ParamMetaIterator) init() {
	i.extractHints()

	block, err := parser.Parse([]byte(i.SQL))
	if err == nil {
		i.buildContexts(AppendContext, block.Statements()...)
	}

	i.cursor = parsly.NewCursor("", []byte(i.SQL), 0)
	i.initMetaTypes(i.SQL)
}

func (i *ParamMetaIterator) buildMetaParam(pos int, name string) {
	context := UnspecifiedContext
	if len(i.contexts) > 0 {
		context = i.contexts[i.counter].Context
	}

	prefix, holder := GetHolderName(name)
	occurrenceIndex := i.occurrences[holder]
	i.occurrences[holder] = occurrenceIndex + 1
	var paramMetaType *ParamMetaType
	if metaType, ok := i.paramMetaTypes[holder]; ok {
		paramMetaType = metaType
	}

	i.paramMeta = &ParamMeta{
		Context:         context,
		Start:           pos,
		End:             pos + len(name),
		FullName:        name,
		Prefix:          prefix,
		Holder:          holder,
		IsVariable:      i.variables[holder],
		OccurrenceIndex: occurrenceIndex,
		MetaType:        paramMetaType,
	}

	i.counter++
}

func (i *ParamMetaIterator) addVariable(selector *expr.Select) {
	_, holderName := GetHolderName(view.NotEmptyOf(selector.FullName, selector.ID))
	if builtInMethods[holderName] {
		return
	}

	i.variables[holderName] = true
}

func (i *ParamMetaIterator) extractHints() {
	hints := ExtractParameterHints(i.SQL)
	for index, hint := range hints {
		i.hints[hint.Parameter] = hints[index]
	}

	i.SQL = RemoveParameterHints(i.SQL, hints)
}
