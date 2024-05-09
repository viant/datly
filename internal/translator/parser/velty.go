package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"strings"
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

			input := string(cur.Input[cur.Pos:])
			if strings.HasPrefix(strings.TrimSpace(input), "$View.ParentJoinOn") {
				if index := strings.Index(input, ")"); index != -1 {
					cur.Pos += index + 1
					fromNode.Unparsed += input[:index]
				}
			}

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
