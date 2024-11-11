package ast

import "fmt"

func (s *Assign) Generate(builder *Builder) (err error) {
	if builder.AssignNotifier != nil {
		newExpr, err := builder.AssignNotifier(s)
		if err != nil {
			return err
		}

		if newExpr != nil && newExpr != s {
			return newExpr.Generate(builder)
		}
	}

	switch builder.Lang {
	case LangVelty:
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

		callExpr, ok := s.Expression.(*CallExpr)
		if ok && callExpr.Name == "IndexBy" && builder.Options.Lang == LangGO {
			if builder.IndexByCode == nil {
				builder.IndexByCode = builder.NewBuilder()
			}
			indexBuilder := builder.IndexByCode
			asIdent, _ := s.Holder.(*Ident)
			if holder := asIdent.Holder; holder != "" {
				indexBuilder.WriteString(holder + ".")
			}
			indexBuilder.WriteString(asIdent.Name)
			indexBuilder.WriteString(" = ")
			if err = s.Expression.Generate(indexBuilder); err != nil {
				return err
			}
			indexBuilder.WriteString("\n")
			return nil
		}

		if err = builder.WriteString("\n"); err != nil {
			return err
		}
		asIdent, ok := s.Holder.(*Ident)
		wasDeclared := true
		if ok {
			wasDeclared = builder.State.IsDeclared(asIdent.Name)
		}

		if err = s.Holder.Generate(builder); err != nil {
			return err
		}

		for _, holder := range s.ExtraHolders {
			if err = builder.WriteString(", "); err != nil {
				return err
			}

			if err = holder.Generate(builder); err != nil {
				return err
			}
		}

		if err = s.appendGoAssignToken(builder, wasDeclared); err != nil {
			return err
		}

		if err = s.Expression.Generate(builder); err != nil {
			return err
		}

		if !wasDeclared {
			builder.State.DeclareVariable(asIdent.Name)
		}
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
