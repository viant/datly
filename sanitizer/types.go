package sanitizer

import (
	"bytes"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/sqlx/io/read/cache/ast"
	"strconv"
	"strings"
)

var resetWords = []string{"AND", "OR", "WITH", "HAVING", "LIMIT", "OFFSET", "WHERE", "SELECT", "UNION", "ALL", "AS", "BETWEEN"}

func (i *ParamMetaIterator) initMetaTypes(SQL string) []string {
	var typer option.Typer
	var untyped []string
	previouslyMatched := -1

	cursor := parsly.NewCursor("", []byte(SQL), 0)

	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, forEachMatcher, ifMatcher, assignMatcher, elseIfMatcher, elseMatcher, endMatcher, commentBlockMatcher, doubleQuoteStringMatcher, singleQuoteStringMatcher, boolTokenMatcher, boolMatcher, numberMatcher, parenthesesBlockMatcher, selectorMatcher, fullWordMatcher, anyMatcher)
		switch matched.Code {
		case numberToken:
			text := matched.Text(cursor)
			typer = i.asNumberTyper(text)
		case boolToken:
			typer = option.NewLiteralType(ast.BoolType)
		case stringToken:
			typer = option.NewLiteralType(ast.StringType)
		case parenthesesBlockToken:
			sqlFragment := matched.Text(cursor)
			untypedInBlock := i.initMetaTypes(sqlFragment[1 : len(sqlFragment)-1])
			if !isVeltyMatchToken(previouslyMatched) {
				untyped = append(untyped, untypedInBlock...)
			}

		case parsly.EOF, anyToken:
			//Do nothing
		default:
			text := matched.Text(cursor)
			//shouldReset := i.isResetKeyword(text)
			//if shouldReset {
			//	typer = nil
			//	untyped = []string{}
			//}

			if i.canBeParam(text) {
				prefix, paramName := GetHolderName(text)
				if prefix == keywords.ParamsMetadataKey {
					continue
				}

				if i.updateParamMetaType(paramName) {
					untyped = append(untyped, paramName)
				}
			} else {
				typer = newColumnTyper(text, typer)
			}
		}

		if typer != nil {
			for _, param := range untyped {
				metaType := i.getOrCreateParamMetaType(param)
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

func newColumnTyper(text string, previous option.Typer) option.Typer {
	if strings.EqualFold(text, OrKeyword) || strings.EqualFold(text, AndKeyword) {
		return nil
	}

	if strings.EqualFold(text, InKeyword) {
		return previous
	}

	if isSQLKeyword(text) {
		return nil
	}

	return &option.ColumnType{ColumnName: strings.ToLower(text)}
}

func (i *ParamMetaIterator) isResetKeyword(text string) bool {
	for _, word := range resetWords {
		if strings.EqualFold(text, word) {
			return true
		}
	}

	return false
}

func (i *ParamMetaIterator) asNumberTyper(text string) option.Typer {
	_, err := strconv.Atoi(text)
	if err == nil {
		return option.NewLiteralType(ast.IntType)
	}

	return option.NewLiteralType(ast.Float64Type)
}

func isVeltyMatchToken(matched int) bool {
	switch matched {
	case endToken, elseToken, assignToken, forEachToken, ifToken:
		return true
	}

	return false
}

func (i *ParamMetaIterator) isParameter(paramName string) bool {
	if isVariable := i.variables[paramName]; isVariable {
		return false
	}

	switch paramName {
	case keywords.Criteria[1:], keywords.SelectorCriteria[1:], keywords.Pagination[1:], keywords.ColumnsIn[1:]:
		return false
	}

	return true
}

func (i *ParamMetaIterator) getOrCreateParamMetaType(paramName string) *ParamMetaType {
	meta, ok := i.paramMetaTypes[paramName]
	if !ok {
		meta = &ParamMetaType{}
		i.paramMetaTypes[paramName] = meta
	}

	return meta
}

func (i *ParamMetaIterator) updateParamMetaType(paramName string) (wasParam bool) {
	if !i.isParameter(paramName) {
		return false
	}

	var hint string
	if paramHint, ok := i.hints[paramName]; ok {
		hint = paramHint.Hint
	}

	if hint == "" {
		return true
	}

	metaType := i.getOrCreateParamMetaType(paramName)
	jsonHint, SQL := SplitHint(hint)
	if jsonHint != "" {
		metaType.Hint = append(metaType.Hint, jsonHint)
	}

	if SQL != "" {
		metaType.SQL = append(metaType.SQL, SQL)
	}

	return true
}

func (i *ParamMetaIterator) canBeParam(text string) bool {
	firstLetter := bytes.ToUpper([]byte{text[0]})[0]
	if (firstLetter < 'A' || firstLetter > 'Z') && firstLetter != '$' {
		return false
	}

	return firstLetter == '$'
}
