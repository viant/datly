package parameter

import "github.com/viant/parsly"

type AnyMatcher struct {
}

func NewAnyMatcher() parsly.Matcher {
	return &AnyMatcher{}
}

func (a *AnyMatcher) Match(cursor *parsly.Cursor) (matched int) {
	if cursor.Pos < cursor.InputSize {
		return 1
	}
	return 0
}
