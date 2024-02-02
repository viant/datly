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
		match := cur.MatchOne(IfBlockMatcher)
		if match.Code == IfBlockToken {
			fromNode.Unparsed = match.Text(cur)
			return nil
		}

		return err
	})
}
