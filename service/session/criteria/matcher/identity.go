package matcher

import (
	"github.com/viant/parsly"
)

type identity struct{}

//Match matches a string
func (n *identity) Match(cursor *parsly.Cursor) (matched int) {
	input := cursor.Input
	pos := cursor.Pos
	if startsWithCharacter := IsLetter(input[pos]); startsWithCharacter {
		pos++
		matched++
	} else {
		return 0
	}

	size := len(input)
	for i := pos; i < size; i++ {
		switch input[i] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_':
			matched++
			continue
		default:
			if IsLetter(input[i]) {
				matched++
				continue
			}

			return matched
		}
	}

	return matched
}

func IsLetter(b byte) bool {
	if (b < 'a' || b > 'z') && (b < 'A' || b > 'Z') {
		return false
	}
	return true
}

//NewIdentity creates a string matcher
func NewIdentity() *identity {
	return &identity{}
}
