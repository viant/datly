package template

import (
	"github.com/viant/datly/internal/translator/parser/matchers"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken = iota
	anyToken
	quotesToken
	doubleQuotesToken

	commentBlockToken
	placeholderToken
	selectorBlockToken
	selectorToken
	whitespaceTerminatorToken
)

var anyMatcher = parsly.NewToken(anyToken, "Any", NewAnyMatcher())
var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var singleQuotesMatcher = parsly.NewToken(quotesToken, "Quotes", matcher.NewBlock('\'', '\'', '\\'))
var doubleQuotesMatcher = parsly.NewToken(doubleQuotesToken, "Double quotes", matcher.NewBlock('"', '"', '\\'))
var commentBlockMatcher = parsly.NewToken(commentBlockToken, "Comment block", matcher.NewSeqBlock("/*", "*/"))
var placeholderMatcher = parsly.NewToken(placeholderToken, "Placeholder", matcher.NewByte('?'))
var selectorBlockMatcher = parsly.NewToken(selectorBlockToken, "QuerySelector block", matcher.NewSeqBlock("${", "}"))
var selectorMatcher = parsly.NewToken(selectorToken, "QuerySelector", matcher.NewByte('$'))
var whitespaceTerminatorMatcher = parsly.NewToken(whitespaceTerminatorToken, "Word", matchers.NewWordMatcher(true))
