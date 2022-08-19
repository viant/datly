package sanitizer

import (
	"github.com/viant/datly/cmd/option"
	"github.com/viant/parsly"
	"strings"
)

func SplitHint(hint string) (marshal string, SQL string) {
	if strings.HasPrefix(hint, "/*") {
		hint = hint[2:]
	}

	if strings.HasSuffix(hint, "*/") {
		hint = hint[:len(hint)-2]
	}

	hint = strings.TrimSpace(hint)

	//TODO: replace with parsly
	index := strings.LastIndex(hint, "}")
	if index != -1 {
		return strings.TrimSpace(hint[:index+1]), strings.TrimSpace(hint[index+1:])
	}

	return "", hint
}

func ExtractParameterHints(text string) option.ParameterHints {
	cursor := parsly.NewCursor("", []byte(text), 0)
	var hints = make([]*option.ParameterHint, 0)
	matcher := NewParamMatcher()

	for cursor.Pos < cursor.InputSize {
		paramName, pos := matcher.TryMatchParam(cursor)
		if pos == -1 {
			cursor.Pos++
			continue
		}

		matched := cursor.MatchAfterOptional(whitespaceMatcher, commentBlockMatcher)
		if matched.Code != commentBlockToken {
			continue
		}

		_, holder := getHolderName(paramName)
		hints = append(hints, &option.ParameterHint{
			Parameter: holder,
			Hint:      matched.Text(cursor),
		})
	}

	return hints
}

func RemoveParameterHints(text string, hints option.ParameterHints) string {
	var pairs = []string{}
	for _, v := range hints {
		pairs = append(pairs, v.Hint, "")
	}
	replacer := strings.NewReplacer(pairs...)
	return replacer.Replace(text)
}
