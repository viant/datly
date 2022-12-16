package sanitize

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
		SQL   string
		Hint  string
	}
)

func NewIterator(SQL string, hints map[string]*ParameterHint, consts map[string]interface{}) *ParamMetaIterator {
	if consts == nil {
		consts = map[string]interface{}{}
	}

	result := &ParamMetaIterator{
		SQL:            SQL,
		assignedVars:   map[string]bool{},
		occurrences:    map[string]int{},
		paramMetaTypes: map[string]*ParamMetaType{},
		paramMatcher:   NewParamMatcher(),
		hints:          hints,
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

func (it *ParamMetaIterator) buildContexts(context Context, statements ...ast.Statement) {

outer:
	for _, statement := range statements {
		if statement == nil {
			continue
		}

		switch actual := statement.(type) {
		case *expr.Select:
			it.contexts = append(it.contexts, NewParamContext(view.FirstNotEmpty(actual.FullName, actual.ID), context))

			for actual.X != nil {
				xSelect, ok := actual.X.(*expr.Select)
				if ok {
					actual = xSelect
					continue
				}

				asFunc, ok := actual.X.(*expr.Call)
				if ok {
					for _, arg := range asFunc.Args {
						it.buildContexts(FuncContext, arg)
					}
				}

				asSlice, ok := actual.X.(*expr.SliceIndex)
				if ok {
					it.buildContexts(context, asSlice.X, asSlice.Y)
				}

				continue outer
			}

		case *expr.Parentheses:
			it.buildContexts(context, actual.P)
		case *expr.Unary:
			it.buildContexts(context, actual.X)
		case *expr.Binary:
			it.buildContexts(context, actual.X, actual.Y)
		case *stmt.ForEach:
			it.addVariable(actual.Item)
			it.buildContexts(ForEachContext, actual.Item, actual.Set)
			it.buildContexts(AppendContext, actual.Body.Stmt...)
		case *stmt.If:
			it.buildContexts(IfContext, actual.Condition)
			it.buildContexts(AppendContext, actual.Body.Stmt...)
			if actual.Else != nil {
				it.buildContexts(IfContext, actual.Else)
			}
		case *stmt.Statement:
			selector, ok := actual.X.(*expr.Select)
			if ok {
				it.addVariable(selector)
			}

			it.addVariable(selector)
			it.buildContexts(SetContext, actual.X, actual.Y)
		}
	}
}

func (it *ParamMetaIterator) Has() bool {
	if it.cursor.Pos >= it.cursor.InputSize && it.paramMeta == nil {
		return false
	}

	var SQLKeyword string

	beforeMatchedWhitespace := it.cursor.Pos
	for it.cursor.Pos < it.cursor.InputSize {
		it.cursor.MatchOne(whitespaceMatcher)
		afterMatchedWhitespace := it.cursor.Pos

		param, pos := it.paramMatcher.TryMatchParam(it.cursor)
		if pos == -1 {
			if beforeMatchedWhitespace == afterMatchedWhitespace {
				it.cursor.Pos++
			} else {
				matchedKeyword, ok := it.matchKeyword()
				if ok {
					SQLKeyword = matchedKeyword
				} else {
					it.cursor.Pos++
				}
			}

			continue
		}

		if method, ok := it.tryBuildParam(SQLKeyword, param, pos); !ok {
			it.cursor.Pos = afterMatchedWhitespace + len(method) + 1
			continue
		}

		_, holderName := GetHolderName(param)
		it.cursor.Pos = afterMatchedWhitespace + len(holderName) + 1
		return true
	}

	return false
}

func (it *ParamMetaIterator) tryBuildParam(SQLKeyword string, param string, pos int) (string, bool) {
	index := it.counter
	it.counter++

	_, name := GetHolderName(param)
	occurrenceIndex := it.occurrences[name]
	it.occurrences[name] = occurrenceIndex + 1

	if builtInMethods[name] {
		return name, false
	}

	it.buildMetaParam(index, occurrenceIndex, pos, param, SQLKeyword)
	return "", true
}

func (it *ParamMetaIterator) Next() *ParamMeta {
	meta := it.paramMeta
	it.paramMeta = nil

	return meta
}

func (it *ParamMetaIterator) init() {
	it.extractHints()

	block, err := parser.Parse([]byte(it.SQL))
	if err == nil {
		it.buildContexts(AppendContext, block.Statements()...)
	}

	it.cursor = parsly.NewCursor("", []byte(it.SQL), 0)
	it.initMetaTypes(it.SQL)
}

func (it *ParamMetaIterator) buildMetaParam(index, occurrence, pos int, raw, SQLKeyword string) {
	context := UnspecifiedContext
	if len(it.contexts) > 0 {
		context = it.contexts[index].Context
	}

	prefix, holder := GetHolderName(raw)

	var paramMetaType *ParamMetaType
	if metaType, ok := it.paramMetaTypes[holder]; ok {
		paramMetaType = metaType
	}

	it.paramMeta = &ParamMeta{
		Context:         context,
		Start:           pos,
		End:             pos + len(raw),
		FullName:        raw,
		Prefix:          prefix,
		Holder:          holder,
		IsVariable:      it.assignedVars[holder],
		OccurrenceIndex: occurrence,
		MetaType:        paramMetaType,
		SQLKeyword:      SQLKeyword,
	}
}

func (it *ParamMetaIterator) addVariable(selector *expr.Select) {
	_, holderName := GetHolderName(view.FirstNotEmpty(selector.FullName, selector.ID))
	if builtInMethods[holderName] {
		return
	}

	it.assignedVars[holderName] = true
}

func (it *ParamMetaIterator) extractHints() {
	hints := ExtractParameterHints(it.SQL)
	for index, hint := range hints {
		it.hints[hint.Parameter] = hints[index]
	}

	it.SQL = RemoveParameterHints(it.SQL, hints)
}

func (it *ParamMetaIterator) matchKeyword() (string, bool) {
	it.cursor.MatchOne(whitespaceMatcher)

	match := it.cursor.MatchOne(fullWordMatcher)
	matchedText := match.Text(it.cursor)
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
