package matcher

import (
	"github.com/viant/parsly"
)

type stringMatcher struct {
	quote byte
}

func (m *stringMatcher) Match(cursor *parsly.Cursor) (matched int) {
	input := cursor.Input
	inputSize := len(input)
	pos := cursor.Pos
	value := input[pos]
	if value != m.quote {
		return 0
	}

	matched++
	for i := pos + matched; i < inputSize; i++ {
		value = input[i]
		matched++
		switch value {
		case m.quote: //quotes
			return matched
		}
	}
	return 0
}

func NewStringMatcher(quote byte) *stringMatcher {
	return &stringMatcher{quote: quote}
}
