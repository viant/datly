package ast

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken = iota
	blockToken
	blockStartToken
	whereToken
	whereStartToken
)

var Whitespace = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var Block = parsly.NewToken(blockToken, "Parentheses", matcher.NewBlock('(', ')', '\\'))
var Where = parsly.NewToken(whereToken, "Where", matcher.NewFragmentsFold([]byte("where")))

var BlockStart = parsly.NewToken(blockStartToken, "Block start", matcher.NewTerminator('(', false))
var WhereStartLC = parsly.NewToken(whereStartToken, "Where start", matcher.NewTerminator('W', false))
var WhereStartUC = parsly.NewToken(whereStartToken, "Where start", matcher.NewTerminator('w', false))
