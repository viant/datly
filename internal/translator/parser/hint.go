package parser

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/parsly"
	"github.com/viant/toolbox"
	"strings"
)

//SplitHint split hint into json and optionally SQL
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
		isUtil := false
		SQL = SQL[1:]
		if len(SQL) > 0 && SQL[0] == '!' {
			isUtil = true
			SQL = SQL[1:]
		}
		utilFragment := ""
		if isUtil {
			utilFragment = `, "Util":true `
		}
		if statCode := toolbox.AsInt(SQL[:3]); statCode > 0 {
			SQL = SQL[3:]
			jsonHint = fmt.Sprintf(`{"Required":true, "StatusCode": %v%s}`, statCode, utilFragment)
		} else {
			jsonHint = fmt.Sprintf(`{"Required":true%s}`, utilFragment)
		}
	}
	return jsonHint, SQL
}

func TryUnmarshalHint(hint string, any interface{}) error {
	hint = strings.TrimSpace(hint)
	if hint == "" {
		return nil
	}

	return hintToStruct(hint, any)
}

func hintToStruct(encoded string, aStructPtr interface{}) error {
	encoded = strings.ReplaceAll(encoded, "/*", "")
	encoded = strings.ReplaceAll(encoded, "*/", "")
	encoded = strings.TrimSpace(encoded)
	return json.Unmarshal([]byte(encoded), aStructPtr)
}

func ExtractParameterHints(text string, state *inference.State) error {
	cursor := parsly.NewCursor("", []byte(text), 0)
	var matcher parameterMatcher
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
		_, holder := SplitSelector(paramSelector)
		parameter := state.Lookup(holder)
		if parameter == nil {
			parameter = &inference.Parameter{}
			parameter.Name = holder
			state.Append(parameter)
		}
		parameter.Hint = matched.Text(cursor)
		JSON, SQL := SplitHint(parameter.Hint)
		info := inference.Parameter{}
		if err := hintToStruct(JSON, &info); err != nil {
			return fmt.Errorf("failed to extract hint for %v %w, %s", holder, err, parameter.Hint)
		}
		parameter.MergeFrom(&info)
		if parameter.SQL == "" {
			parameter.SQL = SQL //TODO detect view vs structql
		}
	}
	return nil
}

func RemoveParameterHints(text string, state inference.State) string {
	var pairs = []string{}
	for _, v := range state {
		if v.Hint == "" {
			continue
		}
		pairs = append(pairs, v.Hint, "")
	}
	replacer := strings.NewReplacer(pairs...)
	return replacer.Replace(text)
}
