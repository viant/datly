package cmd

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken int = iota
	condBlockToken
	exprGroupToken
	importKeywordToken
	quotedToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var condBlockMatcher = parsly.NewToken(condBlockToken, "#if .... #end", matcher.NewSeqBlock("#if", "#end"))
var exprGroupMatcher = parsly.NewToken(exprGroupToken, "( .... )", matcher.NewBlock('(', ')', '\\'))
var importKeywordMatcher = parsly.NewToken(importKeywordToken, "import", matcher.NewFragmentsFold([]byte("import")))
var quotedMatcher = parsly.NewToken(quotedToken, "quoted block", matcher.NewQuote('"', '\\'))
