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

	templateHeaderToken
	templateEndToken
	paramToken
	exprGroupToken
	identityToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var wordMatcher = parsly.NewToken(wordToken, "Word", matchers.NewWordMatcher())
var colonMatcher = parsly.NewToken(colonToken, "Colon", matcher.NewByte(':'))

var squareBracketsMatcher = parsly.NewToken(squareBracketsToken, "Square brackets", matcher.NewBlock('[', ']', '\\'))

var templateHeaderMatcher = parsly.NewToken(templateHeaderToken, "Template header", matcher.NewFragmentsFold([]byte("/*TEMPLATE")))
var templateEndMatcher = parsly.NewToken(templateEndToken, "Template end", matcher.NewFragment("*/"))
var paramMatcher = parsly.NewToken(paramToken, "Parameter", matcher.NewFragmentsFold([]byte("PARAMETER")))

var identityMatcher = parsly.NewToken(identityToken, "Identity", matchers.NewIdentity())
var condBlockMatcher = parsly.NewToken(condBlockToken, "#if .... #end", matcher.NewSeqBlock("#if", "#end"))
var exprGroupMatcher = parsly.NewToken(exprGroupToken, "( .... )", matcher.NewBlock('(', ')', '\\'))
