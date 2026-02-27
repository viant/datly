package decl

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken = iota
	singleQuotedToken
	doubleQuotedToken
	commentBlockToken
	parenthesesBlockToken
	identifierToken
	anyToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var singleQuotedMatcher = parsly.NewToken(singleQuotedToken, "SingleQuote", matcher.NewBlock('\'', '\'', '\\'))
var doubleQuotedMatcher = parsly.NewToken(doubleQuotedToken, "DoubleQuote", matcher.NewBlock('"', '"', '\\'))
var commentBlockMatcher = parsly.NewToken(commentBlockToken, "CommentBlock", matcher.NewSeqBlock("/*", "*/"))
var parenthesesBlockMatcher = parsly.NewToken(parenthesesBlockToken, "Parentheses", matcher.NewBlock('(', ')', '\\'))

var identifierMatcher = parsly.NewToken(identifierToken, "Identifier", &identifierMatch{})
var anyMatcher = parsly.NewToken(anyToken, "Any", &anyMatch{})

type anyMatch struct{}

func (a *anyMatch) Match(cursor *parsly.Cursor) int {
	if cursor.Pos < cursor.InputSize {
		return 1
	}
	return 0
}

type identifierMatch struct{}

func (i *identifierMatch) Match(cursor *parsly.Cursor) int {
	if cursor.Pos >= cursor.InputSize {
		return 0
	}
	b := cursor.Input[cursor.Pos]
	if !isIdentifierStart(b) {
		return 0
	}
	pos := cursor.Pos + 1
	for pos < cursor.InputSize && isIdentifierPart(cursor.Input[pos]) {
		pos++
	}
	return pos - cursor.Pos
}

func isIdentifierStart(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_'
}

func isIdentifierPart(b byte) bool {
	return isIdentifierStart(b) || (b >= '0' && b <= '9')
}
