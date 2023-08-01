package parser

import (
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/functions"
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

	expressionMatcher struct {
		*Template
		cursor      *parsly.Cursor
		expressions Expressions
		counter     int
		occurrences map[string]int
		parameterMatcher
	}

	Expression struct {
		Start           int
		End             int
		FullName        string
		Prefix          string
		Holder          string
		Context         Context
		FnName          string
		IsVariable      bool
		OccurrenceIndex int
		SQLKeyword      string
		Entry           *functions.Entry
	}

	Expressions []*Expression
)

func (m *expressionMatcher) matchExpressions() {
	for m.cursor.Pos < m.cursor.InputSize {
		if !m.matchExpression() {
			break
		}
	}
}

func (m *expressionMatcher) matchExpression() bool {
	if m.cursor.Pos >= m.cursor.InputSize {
		return false
	}
	var SQLKeyword string
	beforeMatchedWhitespace := m.cursor.Pos
	for m.cursor.Pos < m.cursor.InputSize {
		m.cursor.MatchOne(whitespaceMatcher)
		afterMatchedWhitespace := m.cursor.Pos
		param, pos := m.parameterMatcher.TryMatchParam(m.cursor)
		if pos == -1 {
			if beforeMatchedWhitespace == afterMatchedWhitespace {
				m.cursor.Pos++
			} else {
				matchedKeyword, ok := m.matchKeyword()
				if ok {
					SQLKeyword = matchedKeyword
				} else {
					m.cursor.Pos++
				}
			}
			continue
		}

		if method, ok := m.tryBuildExpression(SQLKeyword, param, pos); !ok {
			m.cursor.Pos = afterMatchedWhitespace + len(method) + 1
			continue
		}
		_, holderName := GetHolderNameFromSelector(param)
		m.cursor.Pos = afterMatchedWhitespace + len(holderName) + 1
		return true
	}
	return false
}

func (m *expressionMatcher) tryBuildExpression(SQLKeyword string, param *expr.Select, pos int) (string, bool) {
	index := m.counter
	m.counter++
	_, name := GetHolderNameFromSelector(param)
	occurrenceIndex := m.occurrences[name]
	m.occurrences[name] = occurrenceIndex + 1
	entry, _ := keywords.Get(name)
	//if ok !m.indexPredefined { //not used for sanitized
	//	return name, false
	//}
	m.buildExpression(index, occurrenceIndex, pos, param, SQLKeyword, entry)
	return "", true
}

func (m *expressionMatcher) init() {
	m.cursor = parsly.NewCursor("", []byte(m.SQL), 0)
	m.matchExpressions()
}

func (m *expressionMatcher) buildExpression(index, occurrence, pos int, selector *expr.Select, SQLKeyword string, entry *functions.Entry) {
	context := UnspecifiedContext
	var fnName string
	if len(m.Context) > 0 {
		context = m.Context[index].Context
		fnName = m.Context[index].Function
	}
	raw := shared.FirstNotEmpty(selector.FullName, selector.ID)
	prefix, holder := GetHolderNameFromSelector(selector)
	m.expressions = append(m.expressions, &Expression{
		Context:         context,
		Start:           pos,
		End:             pos + len(raw),
		FullName:        raw,
		Prefix:          prefix,
		Holder:          holder,
		IsVariable:      m.Declared[holder],
		OccurrenceIndex: occurrence,
		SQLKeyword:      SQLKeyword,
		FnName:          fnName,
		Entry:           entry,
	})
}

func (m *expressionMatcher) matchKeyword() (string, bool) {
	m.cursor.MatchOne(whitespaceMatcher)
	match := m.cursor.MatchOne(fullWordMatcher)
	matchedText := match.Text(m.cursor)
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
	return !keywords.Has(name)
}
