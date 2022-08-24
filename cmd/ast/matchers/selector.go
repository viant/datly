package matchers

import (
	"github.com/viant/parsly"
	"github.com/viant/velty/parser/matcher"
)

type selector struct{}

// Match matches a selector
func (n *selector) Match(cursor *parsly.Cursor) (matched int) {
	input := cursor.Input
	pos := cursor.Pos
	size := len(input)
	if input[pos] != '$' {
		return 0
	}
	matched++
	pos++

	if input[pos] == '{' {
		matched++
		pos++
		for i := pos; i < size; i++ {
			matched++
			pos++
			if input[pos] == '}' {
				return matched
			}
		}
	}
	for i := pos; i < size; i++ {
		switch input[i] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '.', ':':
			matched++
			continue
		default:
			if matcher.IsLetter(input[i]) {
				matched++
				continue
			}
			return matched
		}
	}
	return matched
}

// NewSelector creates a selector matcher
func NewSelector() *selector {
	return &selector{}
}
