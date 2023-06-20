package ast

import "fmt"

func (s *Assign) Generate(builder *Builder) (err error) {
	switch builder.Lang {
	case "dsql":
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
	}
	return fmt.Errorf("unsupported option %T %v\n", s, builder.Lang)

}

func NewAssign(holder Expression, expr Expression) *Assign {
	return &Assign{
		Holder:     holder,
		Expression: expr,
	}
}
