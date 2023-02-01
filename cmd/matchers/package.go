package matchers

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

type packageName struct{}

func (p *packageName) Match(cursor *parsly.Cursor) (matched int) {
	var currByte byte
	matchedLetter := false
	for i := cursor.Pos; i < cursor.InputSize; i++ {
		currByte = cursor.Input[i]

		if matcher.IsWhiteSpace(currByte) {
			return p.returnMatched(matched, matchedLetter)
		}

		switch currByte {
		case ';':
			return p.returnMatched(matched, matchedLetter)
		case '_':
			matched++
			continue
		}

		if (currByte >= 'a' && currByte <= 'z') || (currByte >= 'A' && currByte <= 'Z') {
			matched++
			matchedLetter = true
			continue
		}

		return p.returnMatched(matched, matchedLetter)
	}

	return p.returnMatched(matched, matchedLetter)
}

func (p *packageName) returnMatched(matched int, letter bool) int {
	if !letter {
		return 0
	}

	return matched
}

func NewPackageNameMatcher() parsly.Matcher {
	return &packageName{}
}
