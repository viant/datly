package template

import (
	"github.com/viant/parsly"
)

func Parse(SQL string) ([]*Value, error) {
	cursor := parsly.NewCursor("", []byte(SQL), 0)

	var result []*Value
	candidates := []*parsly.Token{singleQuotesMatcher, doubleQuotesMatcher, commentBlockMatcher, placeholderMatcher, selectorBlockMatcher, selectorMatcher, whitespaceTerminatorMatcher, anyMatcher}

	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, candidates...)
		switch matched.Code {
		case placeholderToken:
			result = append(result, &Value{Key: "?", Fragment: "?"})
		case selectorBlockToken:
			text := matched.Text(cursor)
			key := text[2 : len(text)-1]
			result = append(result, &Value{Key: key, Fragment: text})
		case selectorToken:
			if cursor.Input[cursor.Pos] == '.' {
				continue
			}
			matched = cursor.MatchOne(whitespaceTerminatorMatcher)
			aKey := matched.Text(cursor)
			if aKey != "" {
				result = append(result, &Value{Key: aKey, Fragment: "$" + aKey})
			}
		case anyToken:
		case parsly.Invalid:
			return nil, cursor.NewError(candidates...)
		}
	}

	return result, nil
}
