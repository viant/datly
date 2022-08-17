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
	parenthesesBlockToken

	templateHeaderToken
	templateEndToken
	paramToken
	exprGroupToken
	identityToken

	anyToken
	endToken
	elseToken
	assignToken
	elseIfToken
	forEachToken
	ifToken

	scopeBlockToken
	numberToken
	boolToken
	stringToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var wordMatcher = parsly.NewToken(wordToken, "Word", matchers.NewWordMatcher(false))
var fullWordMatcher = parsly.NewToken(wordToken, "Word", matchers.NewWordMatcher(true))
var colonMatcher = parsly.NewToken(colonToken, "Colon", matcher.NewByte(':'))
var commentBlockMatcher = parsly.NewToken(commentBlockToken, "Comment", matcher.NewSeqBlock("/*", "*/"))

var squareBracketsMatcher = parsly.NewToken(squareBracketsToken, "Square brackets", matcher.NewBlock('[', ']', '\\'))
var parenthesesBlockMatcher = parsly.NewToken(parenthesesBlockToken, "Parentheses", matcher.NewBlock('(', ')', '\\'))

var templateHeaderMatcher = parsly.NewToken(templateHeaderToken, "Template header", matcher.NewFragmentsFold([]byte("/*TEMPLATE")))
var templateEndMatcher = parsly.NewToken(templateEndToken, "Template end", matcher.NewFragment("*/"))
var paramMatcher = parsly.NewToken(paramToken, "Parameter", matcher.NewFragmentsFold([]byte("PARAMETER")))

var identityMatcher = parsly.NewToken(identityToken, "Identity", matchers.NewIdentity())
var selectorMatcher = parsly.NewToken(selectorToken, "$...", matchers.NewSelector())

var condBlockMatcher = parsly.NewToken(condBlockToken, "#if .... #end", matcher.NewSeqBlock("#if", "#end"))
var exprGroupMatcher = parsly.NewToken(exprGroupToken, "( .... )", matcher.NewBlock('(', ')', '\\'))
var scopeBlockMatcher = parsly.NewToken(scopeBlockToken, "{ .... }", matcher.NewBlock('{', '}', '\\'))

var anyMatcher = parsly.NewToken(anyToken, "Any", matchers.NewAny())
var endMatcher = parsly.NewToken(endToken, "End", matcher.NewFragment("#end"))
var elseMatcher = parsly.NewToken(elseToken, "Else", matcher.NewFragment("#else"))
var elseIfMatcher = parsly.NewToken(elseToken, "ElseIf", matcher.NewFragment("#elseif"))
var assignMatcher = parsly.NewToken(assignToken, "Set", matcher.NewFragment("#set"))
var forEachMatcher = parsly.NewToken(forEachToken, "ForEach", matcher.NewFragment("#foreach"))
var ifMatcher = parsly.NewToken(ifToken, "If", matcher.NewFragment("#if"))

var numberMatcher = parsly.NewToken(numberToken, "Number", matcher.NewNumber())
var boolMatcher = parsly.NewToken(boolToken, "Boolean", matcher.NewFragmentsFold([]byte("true"), []byte("false")))
var boolTokenMatcher = parsly.NewToken(boolToken, "Boolean", matcher.NewFragments(
	[]byte("&&"), []byte("||"),
))

var singleQuoteStringMatcher = parsly.NewToken(stringToken, "String", matcher.NewBlock('\'', '\'', '\\'))
var doubleQuoteStringMatcher = parsly.NewToken(stringToken, "String", matcher.NewBlock('"', '"', '\\'))
