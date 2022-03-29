package ast

import (
	"github.com/viant/parsly"
)

func HasWhere(source []byte) bool {
	cursor := parsly.NewCursor("", source, 0)
	candidates := []*parsly.Token{Block}

	matched := cursor.MatchAfterOptional(Whitespace, candidates...)
	if matched.Code == blockToken {
		text := matched.Text(cursor)
		return HasWhere([]byte(text[1 : len(text)-1]))
	}

	candidates = []*parsly.Token{BlockStart, WhereStartUC, WhereStartLC}
outer:
	for {
		matched = cursor.MatchAny(candidates...)
		switch matched.Code {
		case blockStartToken:
			matched = cursor.MatchAfterOptional(Whitespace, Block)
			continue outer
		case whereStartToken:
			matched = cursor.MatchOne(Where)
			if matched.Code == whereToken {
				whitespaceAfter := cursor.MatchOne(Whitespace)
				if whitespaceAfter.Code == whitespaceToken {
					return true
				}
			}
		case parsly.EOF, parsly.Invalid:
			return false
		}
	}
}
