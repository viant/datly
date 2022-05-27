package criteria

import (
	matcher2 "github.com/viant/datly/router/criteria/matcher"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

type Token int

const (
	whitespaceToken int = iota
	parenthesesToken

	andToken
	orToken

	comaToken

	fieldToken

	booleanToken
	intToken
	numericToken
	stringToken
	timeToken

	equalToken
	notEqualToken
	greaterToken
	greaterEqualToken
	lowerToken
	lowerEqualToken
	likeToken
	inToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var parenthesesMatcher = parsly.NewToken(parenthesesToken, "Parentheses", matcher.NewBlock('(', ')', '\\'))

var andMatcher = parsly.NewToken(andToken, "And", matcher.NewFragmentsFold([]byte("and")))
var orMatcher = parsly.NewToken(orToken, "Or", matcher.NewFragmentsFold([]byte("or")))

var comaMatcher = parsly.NewToken(comaToken, "Coma", matcher.NewTerminator(',', true))

var fieldMatcher = parsly.NewToken(fieldToken, "Field", matcher2.NewIdentity())

var booleanMatcher = parsly.NewToken(booleanToken, "Boolean", matcher.NewFragments([]byte("true"), []byte("false")))
var intMatcher = parsly.NewToken(intToken, "Int", matcher2.NewIntMatcher())
var numericMatcher = parsly.NewToken(numericToken, "Float", matcher.NewNumber())
var stringMatcher = parsly.NewToken(stringToken, "String", matcher2.NewStringMatcher('\''))
var timeMatcher = parsly.NewToken(timeToken, "Time", matcher2.NewStringMatcher('\''))

var equalMatcher = parsly.NewToken(equalToken, "Equal", matcher.NewByte('='))
var notEqualMatcher = parsly.NewToken(notEqualToken, "Not equal", matcher.NewFragments([]byte("!="), []byte("<>")))
var greaterMatcher = parsly.NewToken(greaterToken, "Greater", matcher.NewByte('>'))
var greaterEqualMatcher = parsly.NewToken(greaterEqualToken, "Greater or equal", matcher.NewFragment(">="))
var lowerMatcher = parsly.NewToken(lowerToken, "Lower", matcher.NewByte('<'))
var lowerEqualMatcher = parsly.NewToken(lowerEqualToken, "Lower or equal", matcher.NewFragment("<="))
var likeMatcher = parsly.NewToken(likeToken, "Like", matcher.NewFragmentsFold([]byte("like")))
var inMatcher = parsly.NewToken(inToken, "In", matcher.NewFragmentsFold([]byte("in")))
