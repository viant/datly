package matcher

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/parsly"
	"testing"
)

func TestIdentity_Match(t *testing.T) {
	useCases := []struct {
		description string
		input       []byte
		matched     int
	}{
		{
			description: "abc matches",
			input:       []byte("abc test"),
			matched:     3,
		},
		{
			description: "unicode doesn't match",
			input:       []byte("日本語 test"),
			matched:     0,
		},
		{
			description: "underscore matches",
			input:       []byte("ABc_test"),
			matched:     8,
		},
		{
			description: "- doesnt match",
			input:       []byte("ABc-test"),
			matched:     3,
		},
		{
			description: "beginning number doesn't match",
			input:       []byte("9ABctest"),
			matched:     0,
		},
	}

	for _, useCase := range useCases {
		matcher := NewIdentity()
		matched := matcher.Match(parsly.NewCursor("", useCase.input, 0))
		assert.Equal(t, useCase.matched, matched, useCase.description)
	}
}
