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

//NewStatementExpression return new statement expr
func NewStatementExpression(expr Expression) *StatementExpression {
	return &StatementExpression{Expression: expr}
}
func (e *CallExpr) Generate(builder *Builder) (err error) {
	caller, err := e.actualCaller(builder)
	if err != nil {
		return err
	}

	if caller.Receiver != nil {
		if err = caller.Receiver.Generate(builder); err != nil {
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
	for i, arg := range caller.Args {
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

func (e *CallExpr) actualCaller(builder *Builder) (*CallExpr, error) {
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
	return fmt.Errorf("unsupported option %T %v\n", s, builder.Lang)
}

func NewIdent(name string) *Ident {
	return &Ident{Name: name}
}
