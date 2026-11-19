package ast

import "fmt"

func NewCallExpr(holder Expression, name string, args ...Expression) *CallExpr {
	return &CallExpr{
		Receiver: holder,
		Name:     name,
		Args:     args,
	}
}

func (s *StatementExpression) Generate(builder *Builder) (err error) {
	if err = builder.WriteIndentedString("\n"); err != nil {
		return err
	}
	return s.Expression.Generate(builder)
}

// NewStatementExpression return new statement expr
func NewStatementExpression(expr Expression) *StatementExpression {
	return &StatementExpression{Expression: expr}
}
func (e *CallExpr) Generate(builder *Builder) (err error) {
	expr, err := e.actualExpr(builder)
	if err != nil {
		return err
	}
	if expr != e {
		return expr.Generate(builder)
	}

	if e.Receiver != nil {

		if err = e.Receiver.Generate(builder); err != nil {
			return err
		}

		if err = builder.WriteString("."); err != nil {
			return err
		}
	}
	if err = builder.WriteString(e.Name); err != nil {
		return err
	}

	if err = builder.WriteString("("); err != nil {
		return err
	}
	for i, arg := range e.Args {
		if i > 0 {
			if err = builder.WriteString(", "); err != nil {
				return err
			}
		}
		if err = arg.Generate(builder); err != nil {
			return err
		}
	}
	if err = builder.WriteString(")"); err != nil {
		return err
	}

	return nil
}

func (e *CallExpr) actualExpr(builder *Builder) (Expression, error) {
	if builder.CallNotifier == nil {
		return e, nil
	}

	notifier, err := builder.CallNotifier(e)
	if err != nil || notifier != nil {
		return notifier, err
	}

	return e, nil
}

func (s *SelectorExpr) Generate(builder *Builder) error {
	return unsupportedOptionUse(builder, s)
}

func unsupportedOptionUse(builder *Builder, s Expression) error {
	return fmt.Errorf("unsupported option %T %v\n", s, builder.Lang)
}

func NewIdent(name string) *Ident {
	return &Ident{Name: name}
}

func NewHolderIndent(holder, name string) *Ident {
	ret := &Ident{Name: name, Holder: holder}
	return ret
}
