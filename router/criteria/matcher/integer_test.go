package matcher

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/parsly"
	"testing"
)

func TestIntMatcher_Match(t *testing.T) {
	testCases := []struct {
		input     string
		matched   int
		cursorPos int
	}{
		{
			input:   "0",
			matched: 1,
		},
		{
			input:   "-1",
			matched: 2,
		},
		{
			input:   "-",
			matched: 0,
		},
		{
			input:   "-1000",
			matched: 5,
		},
		{
			input:   "-1000  abcdef",
			matched: 5,
		},
		{
			input:   "-1000abcdef",
			matched: 0,
		},
		{
			input:     "some matched text -1000abcdef",
			matched:   0,
			cursorPos: 18,
		},
		{
			input:     "some matched text -1000",
			matched:   5,
			cursorPos: 18,
		},
	}

	matcher := NewIntMatcher()
	for _, testCase := range testCases {
		cursor := parsly.NewCursor("", []byte(testCase.input), 0)
		cursor.Pos = testCase.cursorPos
		assert.Equal(t, testCase.matched, matcher.Match(cursor), testCase.input)
	}
}
