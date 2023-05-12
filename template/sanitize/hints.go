package sanitize

import (
	"encoding/json"
	"fmt"
	"github.com/viant/parsly"
	"github.com/viant/toolbox"
	"strings"
)

func ExtractHint(text string) string {
	cursor := parsly.NewCursor("", []byte(text), 0)
	matched := cursor.MatchAfterOptional(whitespaceMatcher, commentBlockMatcher)
	if matched.Code == commentBlockToken {
		return matched.Text(cursor)
	}
	return ""
}

func UnmarshalHint(hint string, dest interface{}) (string, error) {
	hint, SQL := SplitHint(hint)
	if hint == "" {
		return SQL, nil
	}

	err := json.Unmarshal([]byte(hint), dest)
	if err != nil {
		return "", fmt.Errorf("invalid %s, %w", hint, err)
	}
	return strings.TrimSpace(SQL), err
}

func SplitHint(hint string) (marshal string, SQL string) {
	if strings.HasPrefix(hint, "/*") {
		hint = hint[2:]
	}

	if strings.HasSuffix(hint, "*/") {
		hint = hint[:len(hint)-2]
	}

	hint = strings.TrimSpace(hint)
	hintCursor := parsly.NewCursor("", []byte(hint), 0)
	matched := hintCursor.MatchOne(scopeBlockMatcher)
	if matched.Code == scopeBlockToken {
		jsonHint := matched.Text(hintCursor)
		return jsonHint, strings.TrimSpace(string(hintCursor.Input[hintCursor.Pos:]))
	}

	SQL = strings.TrimSpace(hint)
	if SQL == "" {
		return "", ""
	}
	jsonHint := ""
	switch SQL[0] {
	case '?':
		SQL = SQL[1:]
		jsonHint = `{"Required":false}`

	case '!':
		SQL = SQL[1:]
		if statCode := toolbox.AsInt(SQL[:3]); statCode > 0 {
			SQL = SQL[3:]
			jsonHint = fmt.Sprintf(`{"Required":true, "StatusCode": %v}`, statCode)
		} else {
			jsonHint = `{"Required":true}`
		}
	}
	return jsonHint, SQL
}

func ExtractParameterHints(text string) ParameterHints {
	cursor := parsly.NewCursor("", []byte(text), 0)
	var hints = make([]*ParameterHint, 0)
	matcher := NewParamMatcher()

	for cursor.Pos < cursor.InputSize {
		paramSelector, pos := matcher.TryMatchParam(cursor)
		if pos == -1 {
			cursor.Pos++
			continue
		}

		matched := cursor.MatchAfterOptional(whitespaceMatcher, commentBlockMatcher)
		if matched.Code != commentBlockToken {
			continue
		}

		_, holder := GetHolderNameFromSelector(paramSelector)
		hints = append(hints, &ParameterHint{
			Parameter: holder,
			Hint:      matched.Text(cursor),
		})
	}

	return hints
}

func RemoveParameterHints(text string, hints ParameterHints) string {
	var pairs = []string{}
	for _, v := range hints {
		pairs = append(pairs, v.Hint, "")
	}
	replacer := strings.NewReplacer(pairs...)
	return replacer.Replace(text)
}
