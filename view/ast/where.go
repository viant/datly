package ast

import (
	"bytes"
	"github.com/viant/parsly"
)

var where = []byte("where")

func ContainsWhereClause(source []byte) bool {
	cursor := parsly.NewCursor("", source, 0)
	candidates := []*parsly.Token{Block}

	matched := cursor.MatchAfterOptional(Whitespace, candidates...)
	if matched.Code == blockToken {
		text := matched.Text(cursor)
		return ContainsWhereClause([]byte(text[1 : len(text)-1]))
	}

	candidates = []*parsly.Token{Block, WhitespaceTerminator}
outer:
	for {
		matched = cursor.MatchAfterOptional(Whitespace, candidates...)
		switch matched.Code {
		case blockToken:
			matched = cursor.MatchAfterOptional(Whitespace, Block)
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
