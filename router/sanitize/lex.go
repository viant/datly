package sanitize

import (
	lMatcher "github.com/viant/datly/router/sanitize/matcher"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken = iota
	numberToken
	booleanLiteralToken
	stringLiteralToken
	binaryOperator
	operatorLogicalToken
	groupToken
	selectorToken
	isToken
	notToken
	nullToken
	inToken
	nextToken
)

var Whitespace = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var NumberLiteral = parsly.NewToken(numberToken, "NumberLiteral", matcher.NewNumber())
var BooleanLiteral = parsly.NewToken(booleanLiteralToken, "BooleanLiteral", matcher.NewFragmentsFold([]byte("true"), []byte("false")))
var StringLiteral = parsly.NewToken(stringLiteralToken, "StringLiteral", matcher.NewQuote('\'', '\''))
var BinaryOperator = parsly.NewToken(binaryOperator, "BinaryOperator", matcher.NewFragments([]byte("="), []byte("<>"), []byte(">="), []byte("<="), []byte("<"), []byte(">")))
var LogicalOperator = parsly.NewToken(operatorLogicalToken, "LogicalOperator", matcher.NewFragmentsFold([]byte("OR"), []byte("AND")))
var Group = parsly.NewToken(groupToken, "Group", matcher.NewBlock('(', ')', 0))
var SelectorMatch = parsly.NewToken(selectorToken, "Selector", lMatcher.NewIdentity())

var IsKeyword = parsly.NewToken(isToken, "IsKeyword", matcher.NewFragmentsFold([]byte("is")))
var NotKeyword = parsly.NewToken(notToken, "NotKeyword", matcher.NewFragmentsFold([]byte("not")))
var NullKeyword = parsly.NewToken(nullToken, "NullKeyword", matcher.NewFragmentsFold([]byte("null")))

var InKeyword = parsly.NewToken(inToken, "InKeyword", matcher.NewFragmentsFold([]byte("in")))
var Next = parsly.NewToken(nextToken, "Next", matcher.NewChar(','))
