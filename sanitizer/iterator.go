package sanitizer

import (
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	"github.com/viant/velty/parser"
	"strings"
)

const (
	UnspecifiedContext Context = iota
	SetContext
	IfContext
	ForEachContext
	AppendContext
	FuncContext
)

const (
	InKeyword  = "in"
	OrKeyword  = "or"
	AndKeyword = "and"
)

var sqlKeywords = []string{InKeyword, OrKeyword, AndKeyword, "where", "from", "limit", "offset", "having", "select", "update", "delete", "case", "when", "then", "coalesce"}

type (
	Context int

	ParamMetaIterator struct {
		SQL    string
		cursor *parsly.Cursor

		paramMeta      *ParamMeta
		contexts       []*ParamContext
		counter        int
		assignedVars   map[string]bool
		occurrences    map[string]int
		paramMetaTypes map[string]*ParamMetaType
		hints          map[string]*ParameterHint
		paramMatcher   *ParamMatcher
		consts         map[string]interface{}
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
		SQLKeyword      string
	}

	ParamMetaType struct {
		Typer []Typer
		SQL   []string
		Hint  []string
	}
)

func NewIterator(SQL string, hints ParameterHints, consts map[string]interface{}) *ParamMetaIterator {
	if consts == nil {
		consts = map[string]interface{}{}
	}

	result := &ParamMetaIterator{
		SQL:            SQL,
		assignedVars:   map[string]bool{},
		occurrences:    map[string]int{},
		paramMetaTypes: map[string]*ParamMetaType{},
		paramMatcher:   NewParamMatcher(),
		hints:          hints.Index(),
		consts:         consts,
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

outer:
	for _, statement := range statements {
		switch actual := statement.(type) {
		case *expr.Select:
			i.contexts = append(i.contexts, NewParamContext(view.NotEmptyOf(actual.FullName, actual.ID), context))

			for actual.X != nil {
				xSelect, ok := actual.X.(*expr.Select)
				if ok {
					actual = xSelect
					continue
				}

				asFunc, ok := actual.X.(*expr.Call)
				if ok {
					for _, arg := range asFunc.Args {
						i.buildContexts(FuncContext, arg)
					}
				}

				continue outer
			}

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

	var SQLKeyword string

	for i.cursor.Pos < i.cursor.InputSize {
		i.cursor.MatchOne(whitespaceMatcher)

		beforeMatch := i.cursor.Pos
		param, pos := i.paramMatcher.TryMatchParam(i.cursor)
		if pos == -1 {
			matchedKeyword, ok := i.matchKeyword()
			if ok {
				SQLKeyword = matchedKeyword
			} else {
				i.cursor.Pos++
			}

			continue
		}

		if method, ok := i.tryBuildParam(SQLKeyword, param, pos); !ok {
			i.cursor.Pos = beforeMatch + len(method) + 1
			continue
		}

		return true
	}

	return false
}

func (i *ParamMetaIterator) tryBuildParam(SQLKeyword string, param string, pos int) (string, bool) {
	index := i.counter
	i.counter++

	_, name := GetHolderName(param)
	occurrenceIndex := i.occurrences[name]
	i.occurrences[name] = occurrenceIndex + 1

	if builtInMethods[name] {
		return name, false
	}

	i.buildMetaParam(index, occurrenceIndex, pos, param, SQLKeyword)
	return "", true
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

func (i *ParamMetaIterator) buildMetaParam(index, occurrence, pos int, raw, SQLKeyword string) {
	context := UnspecifiedContext
	if len(i.contexts) > 0 {
		context = i.contexts[index].Context
	}

	prefix, holder := GetHolderName(raw)

	var paramMetaType *ParamMetaType
	if metaType, ok := i.paramMetaTypes[holder]; ok {
		paramMetaType = metaType
	}

	i.paramMeta = &ParamMeta{
		Context:         context,
		Start:           pos,
		End:             pos + len(raw),
		FullName:        raw,
		Prefix:          prefix,
		Holder:          holder,
		IsVariable:      i.assignedVars[holder],
		OccurrenceIndex: occurrence,
		MetaType:        paramMetaType,
		SQLKeyword:      SQLKeyword,
	}
}

func (i *ParamMetaIterator) addVariable(selector *expr.Select) {
	_, holderName := GetHolderName(view.NotEmptyOf(selector.FullName, selector.ID))
	if builtInMethods[holderName] {
		return
	}

	i.assignedVars[holderName] = true
}

func (i *ParamMetaIterator) extractHints() {
	hints := ExtractParameterHints(i.SQL)
	for index, hint := range hints {
		i.hints[hint.Parameter] = hints[index]
	}

	i.SQL = RemoveParameterHints(i.SQL, hints)
}

func (i *ParamMetaIterator) matchKeyword() (string, bool) {
	i.cursor.MatchOne(whitespaceMatcher)

	match := i.cursor.MatchOne(fullWordMatcher)
	matchedText := match.Text(i.cursor)
	if isSQLKeyword(matchedText) {
		return matchedText, true
	}

	return "", false
}

func isSQLKeyword(value string) bool {
	for _, candidate := range sqlKeywords {
		if strings.EqualFold(candidate, value) {
			return true
		}
	}

	return false
}

func CanBeParam(name string) bool {
	canBe, ok := builtInMethods[name]
	return !(canBe && ok)
}
