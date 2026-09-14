package compile

import (
	"strings"

	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	veltyparser "github.com/viant/velty/parser"
)

func parseReadSQL(sql string) (*query.Select, error) {
	offset := 0
	parsed, err := sqlparser.ParseQuery(sql, sqlparser.WithStructuralValidation(), sqlparser.WithErrorHandler(func(err error, cursor *parsly.Cursor, destination any) error {
		if from, ok := destination.(*query.From); ok && readPredicateSuffix(cursor, from) {
			return nil
		}
		if cursor != nil {
			offset = cursor.Pos
		}
		return err
	}))
	if err != nil {
		return nil, &Error{Code: CodeSQLParse, Offset: offset, Cause: err}
	}
	return parsed, nil
}

// readPredicateSuffix retains a parser-owned expression after FROM in the
// existing unparsed suffix slot, so source rewriting cannot erase predicates.
func readPredicateSuffix(cursor *parsly.Cursor, from *query.From) bool {
	if cursor == nil || from == nil {
		return false
	}
	start := cursor.Pos
	for cursor.Pos < len(cursor.Input) && (cursor.Input[cursor.Pos] == ' ' || cursor.Input[cursor.Pos] == '\n' || cursor.Input[cursor.Pos] == '\r' || cursor.Input[cursor.Pos] == '\t') {
		cursor.Pos++
	}
	expressionStart := cursor.Pos
	if cursor.Pos >= len(cursor.Input) || cursor.Input[cursor.Pos] != '$' {
		cursor.Pos = start
		return false
	}
	cursor.Pos++
	selector, err := veltyparser.MatchSelector(cursor)
	if err != nil || selector == nil || selector.ID != "predicate" {
		cursor.Pos = start
		return false
	}
	from.Unparsed = strings.TrimSpace(from.Unparsed + " " + string(cursor.Input[expressionStart:cursor.Pos]))
	return true
}
