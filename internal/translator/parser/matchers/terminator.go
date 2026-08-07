package matchers

import (
	"bytes"
	"github.com/viant/parsly"
)

type stringTerminatorMatcher struct {
	value []byte
}

type anyStringTerminatorMatcher struct {
	values [][]byte
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

func (t *anyStringTerminatorMatcher) Match(cursor *parsly.Cursor) (matched int) {
	for i := cursor.Pos; i < cursor.InputSize; i++ {
		for _, value := range t.values {
			if len(value) == 0 || len(value) > cursor.InputSize-i {
				continue
			}
			if bytes.Equal(cursor.Input[i:i+len(value)], value) {
				return matched
			}
		}
		matched++
	}
	return 0
}

func NewStringTerminator(by string) *stringTerminatorMatcher {
	return &stringTerminatorMatcher{value: []byte(by)}
}

func NewAnyStringTerminator(values ...string) *anyStringTerminatorMatcher {
	ret := &anyStringTerminatorMatcher{}
	for _, value := range values {
		if value == "" {
			continue
		}
		ret.values = append(ret.values, []byte(value))
	}
	return ret
}
