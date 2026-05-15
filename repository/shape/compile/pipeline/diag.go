package pipeline

import (
	"unicode/utf8"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
)

func StatementSpan(sqlText string, stmt *dqlstmt.Statement) dqlshape.Span {
	if stmt == nil {
		return pointSpan(sqlText, 0)
	}
	return pointSpan(sqlText, stmt.Start)
}

func pointSpan(text string, offset int) dqlshape.Span {
	start := positionAt(text, offset)
	end := start
	return dqlshape.Span{Start: start, End: end}
}

func positionAt(text string, offset int) dqlshape.Position {
	if offset < 0 {
		offset = 0
	}
	if offset > len(text) {
		offset = len(text)
	}
	line := 1
	char := 1
	index := 0
	for index < offset {
		r, width := utf8.DecodeRuneInString(text[index:])
		if width <= 0 {
			break
		}
		index += width
		if r == '\n' {
			line++
			char = 1
		} else {
			char++
		}
	}
	return dqlshape.Position{Offset: offset, Line: line, Char: char}
}
