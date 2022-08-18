package sanitizer

import (
	"github.com/viant/datly/cmd/ast/matchers"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken = iota
	anyToken
	wordToken
	scopeBlockToken
)

var anyMatcher = parsly.NewToken(anyToken, "Any", matchers.NewAny())
var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var wordMatcher = parsly.NewToken(wordToken, "Word", matchers.NewWordMatcher(true))
var scopeBlockMatcher = parsly.NewToken(scopeBlockToken, "{ .... }", matcher.NewBlock('{', '}', '\\'))
