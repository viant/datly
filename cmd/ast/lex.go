package ast

import (
	"github.com/viant/datly/cmd/ast/matchers"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken int = iota
	wordToken
	colonToken
	condBlockToken
	squareBracketsToken
	commentBlockToken
	selectorToken
	templateHeaderToken
	templateEndToken
	paramToken
	exprGroupToken
	identityToken
	scopeBlockToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var wordMatcher = parsly.NewToken(wordToken, "Word", matchers.NewWordMatcher(false))
var colonMatcher = parsly.NewToken(colonToken, "Colon", matcher.NewByte(':'))
var commentBlockMatcher = parsly.NewToken(commentBlockToken, "Comment", matcher.NewSeqBlock("/*", "*/"))

var squareBracketsMatcher = parsly.NewToken(squareBracketsToken, "Square brackets", matcher.NewBlock('[', ']', '\\'))

var templateHeaderMatcher = parsly.NewToken(templateHeaderToken, "Template header", matcher.NewFragmentsFold([]byte("/*TEMPLATE")))
var templateEndMatcher = parsly.NewToken(templateEndToken, "Template end", matcher.NewFragment("*/"))
var paramMatcher = parsly.NewToken(paramToken, "Parameter", matcher.NewFragmentsFold([]byte("PARAMETER")))

var identityMatcher = parsly.NewToken(identityToken, "Identity", matchers.NewIdentity())
var selectorMatcher = parsly.NewToken(selectorToken, "$...", matchers.NewSelector())

var condBlockMatcher = parsly.NewToken(condBlockToken, "#if .... #end", matcher.NewSeqBlock("#if", "#end"))
var exprGroupMatcher = parsly.NewToken(exprGroupToken, "( .... )", matcher.NewBlock('(', ')', '\\'))
var scopeBlockMatcher = parsly.NewToken(scopeBlockToken, "{ .... }", matcher.NewBlock('{', '}', '\\'))
