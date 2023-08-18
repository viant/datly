package matchers

import (
	"bytes"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

type ifBlock struct{}

var ifKeyword = []byte("#if")

var foreachKeyword = []byte("#foreach")

var endKeyword = []byte("#end")

// Match matches a string
func (n *ifBlock) Match(cursor *parsly.Cursor) (matched int) {
	input := cursor.Input
	pos := cursor.Pos

	size := len(input)

	if !bytes.HasPrefix(input[pos:], ifKeyword) {
		return 0
	}
	pos += len(ifKeyword)
	matched += len(ifKeyword)
	depth := 0
	for i := pos; i < size; i++ {
		ch := input[i]
		matched++
		if matcher.IsWhiteSpace(ch) {
			continue
		}
		prefix := input[i:]
		switch input[i] {
		case '#':
			if bytes.HasPrefix(prefix, endKeyword) {
				if depth == 0 {
					matched += len(endKeyword) - 1
					return matched
				}
				depth--
			} else if bytes.HasPrefix(prefix, ifKeyword) {
				depth++
			} else if bytes.HasPrefix(prefix, foreachKeyword) {
				depth++
			}
		}
	}
	return 0
}

// NewIfBlock creates a string matcher
func NewIfBlock() *ifBlock {
	return &ifBlock{}
}
