package ast

import (
	"strconv"
	"strings"
)

func (s *LiteralExpr) Generate(builder *Builder) error {
	return builder.WriteString(s.Literal)
}

func NewQuotedLiteral(text string) *LiteralExpr {
	if !strings.HasPrefix(text, "\"") {
		text = strconv.Quote(text)
	}
	return &LiteralExpr{text}
}

func NewLiteral(text string) *LiteralExpr {
	return &LiteralExpr{text}
}
