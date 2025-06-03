package parser

import (
	"github.com/viant/datly/shared"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
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
		usesService := strings.Contains(SQL, "$sql.")
		lowerCasedDML := strings.ToLower(SQL)

		quoted := ""

		if index := strings.Index(SQL, `"`); index != -1 {
			quoted = SQL[index+1:]
			if index = strings.Index(quoted, `"`); index != -1 {
				quoted = quoted[:index]
			}
		}
		if usesService && quoted != "" {
			statement.Table = quoted
			if _, ok := tables[statement.Table]; ok {
				continue
			}
			result = append(result, statement.Table)
			tables[statement.Table] = true
			continue
		}
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

	hasRead := false
	for cursor.Pos < cursor.InputSize {
		_ = cursor.MatchOne(whitespaceMatcher)
		beforeMatch := cursor.Pos

		matched := cursor.MatchAfterOptional(whitespaceMatcher, exprMatcher, exprEndMatcher, execStmtMatcher, readStmtMatcher, anyMatcher)
		//tt := matched.Text(cursor)
		//fmt.Println(tt)
		switch matched.Code {
		case exprToken:
			_ = cursor.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher)
		case execStmtToken, readStmtToken:
			isExec := matched.Code == execStmtToken
			if !hasRead {
				hasRead = matched.Code == readStmtToken
			}
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
					Kind:           shared.ExecKindService,
					Selector:       selector,
					SelectorMethod: method,
				}

				statements = append(statements, stmt)
			}
			if !ok {
				pos := cursor.Pos
				for ; pos < len(cursor.Input); pos++ {
					if matcher.IsWhiteSpace(cursor.Input[pos]) {
						cursor.Pos = pos
						break
					}
				}
			}
			nextWhitespace(cursor)
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
	if len(statements) == 1 && !hasRead {
		statements[0].IsExec = true
	}

	return statements
}
