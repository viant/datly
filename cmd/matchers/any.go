package matchers

import (
	"github.com/viant/parsly"
)

type any struct {
}

func (a *any) Match(cursor *parsly.Cursor) (matched int) {
	if cursor.Pos < cursor.InputSize {
		return 1
	}

	return 0
}

func NewAny() parsly.Matcher {
	return &any{}
}
