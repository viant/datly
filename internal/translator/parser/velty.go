package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

func OnVeltyExpression() sqlparser.Option {
	return sqlparser.WithErrorHandler(func(err error, cur *parsly.Cursor, destNode interface{}) error {
		fromNode, ok := destNode.(*query.From)
		if !ok {
			return err
		}
		pos := cur.Pos
		if cur.Input[cur.Pos] == '$' {
			cur.Pos++
			match := cur.MatchOne(scopeBlockMatcher)
			if match.Code != scopeBlockToken {
				cur.Pos = pos
				return err
			}
			cur.MatchAfterOptional(whitespaceMatcher, IfBlockMatcher)
			fromNode.Unparsed = string(cur.Input[pos:cur.Pos])
			return nil
		}

		match := cur.MatchOne(IfBlockMatcher)
		if match.Code == IfBlockToken {
			fromNode.Unparsed = match.Text(cur)
			return nil
		}

		return err
	})
}
