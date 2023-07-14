package parser

import (
	"github.com/viant/datly/cmd/option"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/velty/ast/expr"
	"strings"
)

type Statement struct {
	Start          int
	End            int
	Kind           string
	IsExec         bool
	Selector       *expr.Select
	SelectorMethod string
	Table          string
}

type Statements []*Statement

func (s Statements) IsExec() bool {
	if len(s) == 0 {
		return true //handle does not have SQL
	}
	for _, item := range s {
		if item.IsExec {
			return true
		}
	}
	return false
}

func (s Statements) DMLTables(rawSQL string) []string {
	var tables = make(map[string]bool)
	var result []string
	for _, statement := range s {
		SQL := rawSQL[statement.Start:statement.End]
		lowerCasedDML := strings.ToLower(SQL)
		if strings.Contains(lowerCasedDML, "insert") {
			if stmt, _ := sqlparser.ParseInsert(SQL); stmt != nil {
				if table := sqlparser.Stringify(stmt.Target.X); table != "" {
					statement.Table = table
				}
			}
		} else if strings.Contains(lowerCasedDML, "update") {
			if stmt, _ := sqlparser.ParseUpdate(SQL); stmt != nil {
				if table := sqlparser.Stringify(stmt.Target.X); table != "" {
					statement.Table = table
				}
			}
		} else if strings.Contains(lowerCasedDML, "delete") {
			if stmt, _ := sqlparser.ParseDelete(SQL); stmt != nil {
				if table := sqlparser.Stringify(stmt.Target.X); table != "" {
					statement.Table = table
				}
			}
		}
		if statement.Table == "" {
			continue
		}
		if _, ok := tables[statement.Table]; !ok {
			result = append(result, statement.Table)
		}
		tables[statement.Table] = true
	}
	return result
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
