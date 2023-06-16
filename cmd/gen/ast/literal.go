package ast

func (s *LiteralExpr) Generate(builder *Builder) error {
	return builder.WriteString(s.Literal)
}
