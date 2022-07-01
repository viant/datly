package matchers

import (
	"github.com/viant/parsly"
)

type word struct {
}

func (w *word) Match(cursor *parsly.Cursor) (matched int) {
	var current byte
	for i := cursor.Pos; i < cursor.InputSize; i++ {
		current = cursor.Input[i]
		if current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z' {
			matched++
			continue
		}

		return matched
	}

	return matched
}

func NewWordMatcher() parsly.Matcher {
	return &word{}
}
