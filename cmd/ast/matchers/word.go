package matchers

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

type word struct {
	matchAny bool
}

func (w *word) Match(cursor *parsly.Cursor) (matched int) {
	var current byte
	for i := cursor.Pos; i < cursor.InputSize; i++ {
		current = cursor.Input[i]
		if (current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z') || (w.matchAny && !matcher.IsWhiteSpace(current)) {
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
