package sanitize

import (
	"bytes"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/sqlx/io/read/cache/ast"
	"strconv"
	"strings"
)

var resetWords = []string{"AND", "OR", "WITH", "HAVING", "LIMIT", "OFFSET", "WHERE", "SELECT", "UNION", "ALL", "AS", "BETWEEN"}

func (it *ParamMetaIterator) initMetaTypes(SQL string) []string {
	var typer Typer
	var untyped []string
	previouslyMatched := -1

	cursor := parsly.NewCursor("", []byte(SQL), 0)

	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, insertMatcher, forEachMatcher, ifMatcher, assignMatcher, elseIfMatcher, elseMatcher, endMatcher, commentBlockMatcher, doubleQuoteStringMatcher, singleQuoteStringMatcher, boolTokenMatcher, boolMatcher, numberMatcher, parenthesesBlockMatcher, selectorMatcher, fullWordMatcher, anyMatcher)
		switch matched.Code {
		case numberToken:
			text := matched.Text(cursor)
			typer = it.asNumberTyper(text)
		case boolToken:
			typer = NewLiteralType(ast.BoolType)
		case stringToken:
			typer = NewLiteralType(ast.StringType)
		case parenthesesBlockToken:
			sqlFragment := matched.Text(cursor)
			untypedInBlock := it.initMetaTypes(sqlFragment[1 : len(sqlFragment)-1])
			if !isVeltyMatchToken(previouslyMatched) {
				untyped = append(untyped, untypedInBlock...)
			}

		case insertToken:
			it.detectInsertedTypers(cursor)
		case parsly.EOF, anyToken:
			//Do nothing
		default:
			text := matched.Text(cursor)
			if it.canBeParam(text) {
				prefix, paramName := GetHolderName(text)
				if prefix == keywords.ParamsMetadataKey {
					continue
				}

				if it.updateParamMetaType(paramName) {
					untyped = append(untyped, paramName)
				}
			} else {
				typer = newColumnTyper(text, typer)
			}
		}

		if typer != nil {
			for _, param := range untyped {
				metaType := it.getOrCreateParamMetaType(param)
				metaType.Typer = append(metaType.Typer, typer)
			}

			untyped = nil
		}

		if matched.Token.Code != anyToken {
			previouslyMatched = matched.Token.Code
		}

	}

	return untyped
}

func newColumnTyper(text string, previous Typer) Typer {
	if strings.EqualFold(text, OrKeyword) || strings.EqualFold(text, AndKeyword) {
		return nil
	}

	if strings.EqualFold(text, InKeyword) {
		return previous
	}

	if isSQLKeyword(text) {
		return nil
	}

	return &ColumnType{ColumnName: strings.ToLower(text)}
}

func (it *ParamMetaIterator) isResetKeyword(text string) bool {
	for _, word := range resetWords {
		if strings.EqualFold(text, word) {
			return true
		}
	}

	return false
}

func (it *ParamMetaIterator) asNumberTyper(text string) Typer {
	_, err := strconv.Atoi(text)
	if err == nil {
		return NewLiteralType(ast.IntType)
	}

	return NewLiteralType(ast.Float64Type)
}

func isVeltyMatchToken(matched int) bool {
	switch matched {
	case endToken, elseToken, assignToken, forEachToken, ifToken:
		return true
	}

	return false
}

func (it *ParamMetaIterator) isParameter(paramName string) bool {
	if isVariable := it.assignedVars[paramName]; isVariable {
		return false
	}

	return CanBeParam(paramName)
}

func (it *ParamMetaIterator) getOrCreateParamMetaType(paramName string) *ParamMetaType {
	meta, ok := it.paramMetaTypes[paramName]
	if !ok {
		meta = &ParamMetaType{}
		it.paramMetaTypes[paramName] = meta
	}

	return meta
}

func (it *ParamMetaIterator) updateParamMetaType(paramName string) (wasParam bool) {
	if !it.isParameter(paramName) {
		return false
	}

	var hint string
	if paramHint, ok := it.hints[paramName]; ok {
		hint = paramHint.Hint
	}

	if hint == "" {
		return true
	}

	metaType := it.getOrCreateParamMetaType(paramName)
	jsonHint, SQL := SplitHint(hint)
	if jsonHint != "" {
		metaType.Hint = append(metaType.Hint, jsonHint)
	}

	if SQL != "" {
		metaType.SQL = append(metaType.SQL, SQL)
	}

	return true
}

func (it *ParamMetaIterator) canBeParam(text string) bool {
	firstLetter := bytes.ToUpper([]byte{text[0]})[0]
	if (firstLetter < 'A' || firstLetter > 'Z') && firstLetter != '$' {
		return false
	}

	return CanBeParam(text) && firstLetter == '$'
}

func (it *ParamMetaIterator) detectInsertedTypers(cursor *parsly.Cursor) {
	matched := cursor.MatchAfterOptional(whitespaceMatcher, intoMatcher)
	if matched.Code != intoToken {
		return
	}

	matched = cursor.MatchAfterOptional(whitespaceMatcher, fullWordMatcher, singleQuoteStringMatcher, doubleQuoteStringMatcher, backtickQuoteStringMatcher)
	if matched.Code != wordToken && matched.Code != stringToken {
		return
	}

	matched = cursor.MatchAfterOptional(whitespaceMatcher, parenthesesBlockMatcher)
	if matched.Code != parenthesesBlockToken {
		return
	}

	intoContent := matched.Text(cursor)
	intoContent = intoContent[1 : len(intoContent)-1]

	intoColumns := extractValues(intoContent)
	matched = cursor.MatchAfterOptional(whitespaceMatcher, valuesMatcher)
	if matched.Code != valuesToken {
		return
	}

	matched = cursor.MatchAfterOptional(whitespaceMatcher, parenthesesBlockMatcher)
	if matched.Code != parenthesesBlockToken {
		return
	}

	valuesContent := matched.Text(cursor)
	valuesContent = valuesContent[1 : len(valuesContent)-1]

	values := extractValues(valuesContent)
	it.updateInsertedParameterTypes(intoColumns, values)
}

func (it *ParamMetaIterator) updateInsertedParameterTypes(columns []string, values []string) {
	for i, value := range values {
		if len(columns) <= i {
			return
		}

		if !it.canBeParam(value) {
			continue
		}

		metaType := it.getOrCreateParamMetaType(value[1:])
		metaType.Typer = append(metaType.Typer, &ColumnType{ColumnName: strings.ToLower(columns[i])})
	}
}

func extractValues(content string) []string {
	var result []string

	cursor := parsly.NewCursor("", []byte(content), 0)
	var prevPos int
outer:
	for {
		cursor.MatchOne(whitespaceMatcher)
		prevPos = cursor.Pos

		matched := cursor.MatchOne(comaTerminatorMatcher)
		switch matched.Code {
		case comaTerminatorToken:
			appendValue(&result, content[prevPos:cursor.Pos-1])
		default:
			appendValue(&result, content[prevPos:])
			break outer
		}
	}

	return result
}

func appendValue(result *[]string, value string) {
	*result = append(*result, strings.Trim(value, "'`\""))
}
