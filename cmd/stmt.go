package cmd

import (
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
)

type Statement struct {
	Start    int
	End      int
	Kind     string
	IsExec   bool
	Selector *expr.Select
}

func GetStatements(SQL string) []*Statement {
	cursor := parsly.NewCursor("", []byte(SQL), 0)
	var boundaries []*Statement

	var stmt *Statement

	for cursor.Pos < cursor.InputSize {
		_ = cursor.MatchOne(whitespaceMatcher)
		beforeMatch := cursor.Pos

		matched := cursor.MatchAfterOptional(whitespaceMatcher, exprMatcher, exprEndMatcher, execStmtMatcher, readStmtMatcher, anyMatcher)
		switch matched.Code {
		case exprToken:
			_ = cursor.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher)
		case execStmtToken, readStmtToken:
			isExec := matched.Code == execStmtToken

			if nextWhitespace(cursor) {
				if stmt != nil {
					stmt.End = beforeMatch
				}

				stmt = &Statement{
					Start:  beforeMatch,
					End:    -1,
					IsExec: isExec,
				}

				boundaries = append(boundaries, stmt)
			}
		case anyToken:
			selector, ok := getStmtSelector(matched, cursor)
			if ok {
				if stmt != nil {
					stmt.End = beforeMatch
				}

				stmt = &Statement{
					Start:    beforeMatch,
					End:      -1,
					IsExec:   true,
					Kind:     option.ExecKindService,
					Selector: selector,
				}

				boundaries = append(boundaries, stmt)
			}

		}
	}

	if stmt != nil {
		stmt.End = len(SQL)
	} else if len(boundaries) == 0 {
		boundaries = append(boundaries, &Statement{
			Start: 0,
			End:   len(SQL),
		})
	}

	return boundaries
}

func getStmtSelector(matched *parsly.TokenMatch, cursor *parsly.Cursor) (*expr.Select, bool) {
	text := matched.Text(cursor)
	if text != "$" {
		return nil, false
	}

	selector, err := parser.MatchSelector(cursor)
	if err != nil || selector.ID != keywords.KeySQL || selector.X == nil {
		return nil, false
	}

	aSelector, ok := selector.X.(*expr.Select)
	if !ok || (aSelector.ID != "Insert" && aSelector.ID != "Update") {
		return nil, false
	}

	return aSelector, ok
}

func nextWhitespace(cursor *parsly.Cursor) bool {
	beforeMatch := cursor.Pos
	cursor.MatchOne(whitespaceMatcher)
	return beforeMatch != cursor.Pos
}
