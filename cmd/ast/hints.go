package ast

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/parsly"
	"strings"
)

func RemoveParameterHints(text string, hints option.ParameterHints) string {
	var pairs = []string{}
	for _, v := range hints {
		pairs = append(pairs, v.Hint, "")
	}
	replacer := strings.NewReplacer(pairs...)
	return replacer.Replace(text)
}

func ExtractParameterHints(text string) option.ParameterHints {
	var hints = make([]*option.ParameterHint, 0)
outer:
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '$':
			selExpr := ExtractSelector(text[i:])
			if selExpr == "" {
				continue outer
			}

			_, paramName := getHolderName(selExpr)
			candidate := text[i+len(selExpr):]
			if hint := ExtractHint(candidate); hint != "" {
				hints = append(hints, &option.ParameterHint{Parameter: paramName, Hint: hint})
			}
		}
	}

	return hints
}

func ExtractHint(text string) string {
	cursor := parsly.NewCursor("", []byte(text), 0)
	matched := cursor.MatchAfterOptional(whitespaceMatcher, commentBlockMatcher)
	if matched.Code == commentBlockToken {
		return matched.Text(cursor)
	}
	return ""
}

func UnmarshalHint(hint string, dest interface{}) (string, error) {
	hint = hint[2 : len(hint)-2]
	hint = strings.TrimSpace(hint)

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
	//TODO: replace with parsly

	index := strings.LastIndex(hint, "}")
	if index != -1 {
		return hint[:index+1], hint[index+1:]
	}

	return "", hint
}
