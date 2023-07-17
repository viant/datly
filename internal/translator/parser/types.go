package parser

import (
	"bytes"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/sqlx/io/read/cache/ast"
	"strconv"
	"strings"
)

var resetWords = []string{"AND", "OR", "WITH", "HAVING", "LIMIT", "OFFSET", "WHERE", "SELECT", "UNION", "ALL", "AS", "BETWEEN"}

func (t *Template) DetectTypes(handler func(state *inference.State, parameter string, expression *ExpressionContext)) {
	discoverer := &types{Template: t, _handler: handler}
	discoverer.discover(t.SQL)
}

type types struct {
	*Template
	_handler func(state *inference.State, parameter string, exprs *ExpressionContext)
}

func (t *types) handle(state *inference.State, parameter string, exprs *ExpressionContext) {
	if t.isParameterPath(parameter) {
		return
	}
	t._handler(state, parameter, exprs)
}

func (t *types) discover(SQL string) []string {
	var expr *ExpressionContext
	var untyped []string
	previouslyMatched := -1
	t.discoverWithContext()
	cursor := parsly.NewCursor("", []byte(SQL), 0)
	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, insertMatcher, forEachMatcher, ifMatcher, assignMatcher, elseIfMatcher, elseMatcher, endMatcher, commentBlockMatcher, doubleQuoteStringMatcher, singleQuoteStringMatcher, boolTokenMatcher, boolMatcher, numberMatcher, parenthesesBlockMatcher, selectorMatcher, fullWordMatcher, anyMatcher)
		switch matched.Code {
		case numberToken:
			text := matched.Text(cursor)
			expr = t.asNumberTyper(text)
		case boolToken:
			expr = &ExpressionContext{Type: ast.BoolType}
		case stringToken:
			expr = &ExpressionContext{Type: ast.StringType}
		case parenthesesBlockToken:
			sqlFragment := matched.Text(cursor)
			untypedInBlock := t.discover(sqlFragment[1 : len(sqlFragment)-1])
			if !isVeltyMatchToken(previouslyMatched) {
				untyped = append(untyped, untypedInBlock...)
			}
		case insertToken:
			t.detectInsertedTypers(cursor)
		case parsly.EOF, anyToken:
			//Exec nothing
		default:
			text := matched.Text(cursor)
			shouldReset := t.isResetKeyword(text)
			if shouldReset {
				untyped = []string{}
			}

			if t.canBeParam(text) {
				prefix, paramName := GetHolderName(text)
				if prefix == keywords.ParamsMetadataKey {
					continue
				}

				if t.updateParamMetaType(paramName) {
					untyped = append(untyped, paramName)
				}
			} else {
				expr = newColumnTyper(text, expr)
			}
		}

		if expr != nil {
			for _, param := range untyped {
				t.handle(t.State, param, expr)
			}
			untyped = nil
		}

		if matched.Token.Code != anyToken {
			previouslyMatched = matched.Token.Code
		}
	}
	return untyped
}

func (t *types) discoverWithContext() {
	for _, param := range t.Template.Context {
		name := param.Name
		if strings.HasPrefix(name, "$") {
			name = name[1:]
		}
		if name[0] == '{' && name[len(name)-1] == '}' {
			name = name[1 : len(name)-1]
		}
		if index := strings.Index(name, "Unsafe."); index != -1 {
			name = name[:index]
		}

		if t.isParameter(name) {
			t.handle(t.State, name, param)
		}
	}
}

func (t *types) isParameterPath(name string) bool {
	if index := strings.Index(name, "."); index != -1 {
		holder := name[:index]
		if t.Template.Declared[holder] { //locally defined variable
			return true
		}
		if t.State.Lookup(holder) != nil { //locally defined variable
			return true
		}
	}
	return false
}

func newColumnTyper(text string, previous *ExpressionContext) *ExpressionContext {
	if strings.EqualFold(text, OrKeyword) || strings.EqualFold(text, AndKeyword) {
		return nil
	}

	if strings.EqualFold(text, InKeyword) {
		return previous
	}

	if isSQLKeyword(text) {
		return nil
	}
	return &ExpressionContext{Column: strings.ToLower(text)}
}

func (t *types) isResetKeyword(text string) bool {
	for _, word := range resetWords {
		if strings.EqualFold(text, word) {
			return true
		}
	}

	return false
}

func (t *types) asNumberTyper(text string) *ExpressionContext {
	_, err := strconv.Atoi(text)
	if err == nil {
		return &ExpressionContext{Type: ast.IntType}
	}
	return &ExpressionContext{Type: ast.Float64Type}
}

func isVeltyMatchToken(matched int) bool {
	switch matched {
	case endToken, elseToken, assignToken, forEachToken, ifToken:
		return true
	}

	return false
}

func (t *types) updateParamMetaType(paramName string) (wasParam bool) {
	if !t.isParameter(paramName) {
		return false
	}
	return t.Template.State.Lookup(paramName) != nil
}

func (t *types) isParameter(paramName string) bool {
	if paramName == "" || strings.Contains(paramName, "(") || strings.Contains(paramName, "[") {
		return false
	}
	if isVariable := t.Declared[paramName]; isVariable {
		return false
	}
	return CanBeParam(paramName)
}

func (t *types) canBeParam(text string) bool {
	if len(text) == 0 {
		return false
	}
	firstLetter := bytes.ToUpper([]byte{text[0]})[0]
	if (firstLetter < 'A' || firstLetter > 'Z') && firstLetter != '$' {
		return false
	}
	return CanBeParam(text) && firstLetter == '$'
}

func (t *types) detectInsertedTypers(cursor *parsly.Cursor) {
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
	t.updateInsertedParameterTypes(intoColumns, values)
}

func (t *types) updateInsertedParameterTypes(columns []string, values []string) {
	for i, value := range values {
		if len(columns) <= i {
			return
		}

		if !t.canBeParam(value) {
			continue
		}
		t.handle(t.State, value[1:], &ExpressionContext{Column: strings.ToLower(columns[i])})
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
