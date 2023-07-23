package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
)

//ParameterMatcher represent parameter matchinfo
type parameterMatcher parsly.TokenMatch

func (p *parameterMatcher) TryMatchParam(cursor *parsly.Cursor) (*expr.Select, int) {
	matchedPos := cursor.Pos
	match := cursor.MatchOne(selectorStartMatcher)
	*p = parameterMatcher(*match)
	if p.Code != selectorStartToken {
		return nil, -1
	}
	selector, err := parser.MatchSelector(cursor)
	if err != nil {
		return nil, -1
	}
	return selector, matchedPos
}
