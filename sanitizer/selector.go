package sanitizer

import (
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/velty/parser"
)

type ParamNameHandler func(paramName string, pos int, cursor *parsly.Cursor) (cont bool, revert bool)

type ParamMatcher struct {
	matched *parsly.TokenMatch
}

func NewParamMatcher() *ParamMatcher {
	return &ParamMatcher{}
}

func (p *ParamMatcher) TryMatchParam(cursor *parsly.Cursor) (string, int) {
	matchedPos := cursor.Pos
	p.matched = cursor.MatchOne(selectorStartMatcher)
	if p.matched.Code != selectorStartToken {
		return "", -1
	}

	selector, err := parser.MatchSelector(cursor)
	if err != nil {
		return "", -1
	}

	return view.NotEmptyOf(selector.FullName, selector.ID), matchedPos
}
