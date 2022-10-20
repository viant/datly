package cmd

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken int = iota
	condBlockToken
	exprGroupToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var condBlockMatcher = parsly.NewToken(condBlockToken, "#if .... #end", matcher.NewSeqBlock("#if", "#end"))
var exprGroupMatcher = parsly.NewToken(exprGroupToken, "( .... )", matcher.NewBlock('(', ')', '\\'))
