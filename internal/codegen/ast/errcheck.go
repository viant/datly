package ast

type ErrorCheck struct {
	X Expression
}

func (e *ErrorCheck) Generate(builder *Builder) error {
	switch builder.Options.Lang {
	case LangGO:
		if err := builder.WriteString("if err ="); err != nil {
			return err
		}
		if err := e.X.Generate(builder); err != nil {
			return err
		}
		return builder.WriteString(";err != nil {\nreturn nil, err\n}")
	case LangVelty:
		if err := builder.WriteString("\n"); err != nil {
			return err
		}
		return e.X.Generate(builder)
	}

	return unsupportedOptionUse(builder, e)
}

func NewErrorCheck(expr Expression) *ErrorCheck {
	return &ErrorCheck{X: expr}
}
