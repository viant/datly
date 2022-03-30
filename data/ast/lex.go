package ast

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken = iota
	whitespaceTerminateToken
	blockToken
)

var Whitespace = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var Block = parsly.NewToken(blockToken, "Parentheses", matcher.NewBlock('(', ')', '\\'))
var WhitespaceTerminator = parsly.NewToken(whitespaceTerminateToken, "Whitespace terminate", newTerminatorAny(false, []byte{' ', '\n', '\t', '\r', '\v', '\f', 0x85, 0xA0}))
