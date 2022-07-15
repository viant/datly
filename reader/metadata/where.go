package metadata

import (
	"bytes"
	"github.com/viant/parsly"
)

func ContainsWhereClause(source []byte) bool {
	cursor := parsly.NewCursor("", source, 0)
	candidates := []*parsly.Token{parenthesesMatcher}

	matched := cursor.MatchAfterOptional(whitespaceMatcher, candidates...)
	candidates = []*parsly.Token{parenthesesMatcher, WhitespaceTerminator}
outer:
	for {
		matched = cursor.MatchAfterOptional(whitespaceMatcher, candidates...)
		switch matched.Code {
		case parenthesesToken:
			matched = cursor.MatchAfterOptional(whitespaceMatcher, parenthesesMatcher)
			continue outer
		case whitespaceTerminateToken:
			text := matched.Text(cursor)
			if bytes.EqualFold([]byte(text), where) {
				return true
			}
		case parsly.EOF, parsly.Invalid:
			return bytes.EqualFold([]byte(matched.Text(cursor)), where)
		}
	}
}
