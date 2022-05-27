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
		matched     bool
	}{
		{
			description: "abc matches",
			input:       []byte("abc test"),
			matched:     true,
		},
		{
			description: "unicode doesn't match",
			input:       []byte("日本語 test"),
			matched:     false,
		},
		{
			description: "underscore matches",
			input:       []byte("ABc_test"),
			matched:     true,
		},
		{
			description: "- doesnt match",
			input:       []byte("ABc-test"),
			matched:     false,
		},
		{
			description: "beginning number doesn't match",
			input:       []byte("9ABctest"),
			matched:     false,
		},
	}

	for _, useCase := range useCases {
		matcher := NewIdentity()
		matched := matcher.Match(parsly.NewCursor("", useCase.input, 0))
		assert.Equal(t, useCase.matched, matched > 0, useCase.description)
	}
}
