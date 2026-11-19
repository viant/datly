package ast

func NewBinary(x Expression, op string, y Expression) *BinaryExpr {
	return &BinaryExpr{X: x, Op: op, Y: y}
}

func (s *BinaryExpr) Generate(builder *Builder) (err error) {
	if err := s.X.Generate(builder); err != nil {
		return err
	}
	if err = builder.WriteString(" "); err != nil {
		return err
	}

	if err = builder.WriteString(s.Op); err != nil {
		return err
	}
	if err = builder.WriteString(" "); err != nil {
		return err
	}
	return s.Y.Generate(builder)
}
