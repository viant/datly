package matchers

import (
	"bytes"
	"github.com/viant/parsly"
)

type stringTerminatorMatcher struct {
	value []byte
}

func (t *stringTerminatorMatcher) Match(cursor *parsly.Cursor) (matched int) {
	if len(t.value) >= cursor.InputSize-cursor.Pos {
		return 0
	}

	for i := cursor.Pos; i < cursor.InputSize-len(t.value); i++ {
		if bytes.Equal(cursor.Input[i:i+len(t.value)], t.value) {
			return matched
		}

		matched++
	}

	return 0
}

func NewStringTerminator(by string) *stringTerminatorMatcher {
	return &stringTerminatorMatcher{value: []byte(by)}
}
