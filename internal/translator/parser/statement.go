package parser

import (
	"github.com/viant/datly/cmd/option"
	"github.com/viant/parsly"
	"github.com/viant/velty/ast/expr"
)

type Statement struct {
	Start          int
	End            int
	Kind           string
	IsExec         bool
	Selector       *expr.Select
	SelectorMethod string
}

type Statements []*Statement

func (s Statements) IsExec() bool {
	for _, item := range s {
		if item.IsExec {
			return true
		}
	}
	return false
}

func NewStatements(SQL string) Statements {
	cursor := parsly.NewCursor("", []byte(SQL), 0)
	var statements []*Statement

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

				statements = append(statements, stmt)
			}
		case anyToken:
			selector, method, ok := getStmtSelector(matched, cursor)
			if ok {
				if stmt != nil {
					stmt.End = beforeMatch
				}

				stmt = &Statement{
					Start:          beforeMatch,
					End:            -1,
					IsExec:         true,
					Kind:           option.ExecKindService,
					Selector:       selector,
					SelectorMethod: method,
				}

				statements = append(statements, stmt)
			}

		}
	}

	if stmt != nil {
		stmt.End = len(SQL)
	} else if len(statements) == 0 {
		statements = append(statements, &Statement{
			Start: 0,
			End:   len(SQL),
		})
	}

	return statements
}
