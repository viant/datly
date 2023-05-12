package sanitize

import (
	"github.com/viant/parsly"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
)

type ParamNameHandler func(paramName string, pos int, cursor *parsly.Cursor) (cont bool, revert bool)

type ParamMatcher struct {
	matched *parsly.TokenMatch
}

func NewParamMatcher() *ParamMatcher {
	return &ParamMatcher{}
}

func (p *ParamMatcher) TryMatchParam(cursor *parsly.Cursor) (*expr.Select, int) {
	matchedPos := cursor.Pos
	p.matched = cursor.MatchOne(selectorStartMatcher)
	if p.matched.Code != selectorStartToken {
		return nil, -1
	}

	selector, err := parser.MatchSelector(cursor)
	if err != nil {
		return nil, -1
	}

	return selector, matchedPos
	//return selector, view.FirstNotEmpty(selector.FullName, selector.ID), matchedPos
}
