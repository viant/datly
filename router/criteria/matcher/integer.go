package matcher

import (
	"github.com/viant/parsly"
)

type intMatcher struct {
}

func (_ *intMatcher) Match(cursor *parsly.Cursor) (matched int) {
	if cursor.Pos < cursor.InputSize && cursor.Input[cursor.Pos] == '-' {
		matched++
	}

	if cursor.Pos+matched >= cursor.InputSize || !isNumber(cursor.Input[cursor.Pos+matched]) {
		return 0
	}

	for i := cursor.Pos + matched; i < cursor.InputSize; i++ {
		if isNumber(cursor.Input[i]) {
			matched++
			continue
		}

		if isWhitespace(cursor.Input[i]) {
			return matched
		}

		return 0
	}

	return matched
}

func NewIntMatcher() *intMatcher {
	return &intMatcher{}
}

func isNumber(aByte byte) bool {
	return aByte >= '0' && aByte <= '9'
}

func isWhitespace(b byte) bool {
	return b == ' ' || b == '\n' || b == '\t' || b == '\r' || b == '\v' || b == '\f' || b == 0x85 || b == 0xA0
}
