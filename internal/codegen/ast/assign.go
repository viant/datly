package ast

import "fmt"

func (s *Assign) Generate(builder *Builder) (err error) {
	switch builder.Lang {
	case LangDSQL:
		if err = builder.WriteIndentedString("\n#set("); err != nil {
			return err
		}
		if err = s.Holder.Generate(builder); err != nil {
			return err
		}
		if err = builder.WriteString(" = "); err != nil {
			return err
		}
		if err = s.Expression.Generate(builder); err != nil {
			return err
		}
		if err = builder.WriteString(")"); err != nil {
			return err
		}
		return nil

	case LangGO:
		asIdent, ok := s.Holder.(*Ident)
		if !ok {
			return fmt.Errorf("can't assign to Holder %T", s.Holder)
		}

		wasDeclared := builder.State.IsDeclared(asIdent.Name)

		if err = s.Holder.Generate(builder); err != nil {
			return err
		}

		if err = s.appendGoAssignToken(builder, wasDeclared); err != nil {
			return err
		}

		if err = s.Expression.Generate(builder); err != nil {
			return err
		}

		builder.State.DeclareVariable(asIdent.Name)

		return nil
	}
	return fmt.Errorf("unsupported option %T %v\n", s, builder.Lang)

}

func (s *Assign) appendGoAssignToken(builder *Builder, isDeclared bool) error {
	if isDeclared {
		return builder.WriteString(" = ")
	}

	return builder.WriteString(" := ")
}

func NewAssign(holder Expression, expr Expression) *Assign {
	return &Assign{
		Holder:     holder,
		Expression: expr,
	}
}
