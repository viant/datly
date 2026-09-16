package ast

type (
	DerefExpression struct {
		X Expression
	}

	RefExpression struct {
		X Expression
	}
)

func NewRefExpression(x Expression) *RefExpression {
	return &RefExpression{
		X: x,
	}
}

func NewDerefExpression(x Expression) *DerefExpression {
	return &DerefExpression{X: x}
}

func (s *DerefExpression) Generate(builder *Builder) error {
	switch builder.Options.Lang {
	case LangGO:
		if err := builder.WriteString("*"); err != nil {
			return err
		}

		fallthrough
	case LangVelty:
		return s.X.Generate(builder)
	}

	return unsupportedOptionUse(builder, s)
}

func (s *RefExpression) Generate(builder *Builder) error {
	switch builder.Options.Lang {
	case LangGO:
		if err := builder.WriteString("&"); err != nil {
			return err
		}

		fallthrough
	case LangVelty:
		return s.X.Generate(builder)
	}

	return unsupportedOptionUse(builder, s)
}
