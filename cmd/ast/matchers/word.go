package matchers

import (
	"github.com/viant/parsly"
)

type word struct {
	matchAny bool
}

func (w *word) Match(cursor *parsly.Cursor) (matched int) {
	var current byte
	for i := cursor.Pos; i < cursor.InputSize; i++ {
		current = cursor.Input[i]

		if (current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z') || (w.matchAny && isAllowedSpecialChar(current)) {
			matched++
			continue
		}

		return matched
	}

	return matched
}

func NewWordMatcher(matchAny bool) parsly.Matcher {
	return &word{matchAny: matchAny}
}

func isAllowedSpecialChar(char byte) bool {
	return char == '$' || char == '.' || char == '_' || (char >= '0' && char <= '9')
}
