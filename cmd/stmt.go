package cmd

import "github.com/viant/parsly"

func GetStmtBoundaries(SQL string) []int {
	cursor := parsly.NewCursor("", []byte(SQL), 0)
	var boundaries []int
	for cursor.Pos < cursor.InputSize {
		_ = cursor.MatchOne(whitespaceMatcher)
		beforeMatch := cursor.Pos

		matched := cursor.MatchAfterOptional(whitespaceMatcher, exprMatcher, exprEndMatcher, execStmtMatcher, readStmtMatcher, anyMatcher)
		switch matched.Code {
		case exprToken:
			_ = cursor.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher)
		case execStmtToken, readStmtToken:
			if nextWhitespace(cursor) {
				boundaries = append(boundaries, beforeMatch)
			}
		}
	}

	if len(boundaries) == 0 {
		boundaries = []int{0, len(SQL)}
	} else if boundaries[len(boundaries)-1] != len(SQL) {
		boundaries = append(boundaries, len(SQL))
	}

	return boundaries
}

func nextWhitespace(cursor *parsly.Cursor) bool {
	beforeMatch := cursor.Pos
	cursor.MatchOne(whitespaceMatcher)
	return beforeMatch != cursor.Pos
}
