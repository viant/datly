package ast

type (
	Function struct {
		Receiver *Receiver
		Name     string
		ArgsIn   []*FuncArg
		ArgsOut  []string
		Body     Block
		Return   *ReturnExpr
	}

	Receiver struct {
		Name  string
		Ident *Ident
	}

	FuncArg struct {
		Name  string
		Ident *Ident
	}
)

func (a *FuncArg) Generate(builder *Builder) error {
	switch builder.Lang {
	case LangGO:
		if err := builder.WriteString(a.Name); err != nil {
			return err
		}

		if err := builder.WriteString(" "); err != nil {
			return err
		}

		return a.Ident.Generate(builder)
	}

	return unsupportedOptionUse(builder, a)
}

func (r *Receiver) Generate(builder *Builder) error {
	switch builder.Lang {
	case LangGO:
		if err := builder.WriteString(r.Name); err != nil {
			return err
		}

		if err := builder.WriteString(" "); err != nil {
			return err
		}

		return r.Ident.Generate(builder)
	}

	return unsupportedOptionUse(builder, r)
}

func (f *Function) Generate(builder *Builder) error {
	switch builder.Lang {
	case LangGO:
		if err := builder.WriteIndentedString("\nfunc "); err != nil {
			return err
		}

		if f.Receiver != nil {
			if err := builder.WriteString("( "); err != nil {
				return err
			}

			if err := f.Receiver.Generate(builder); err != nil {
				return err
			}

			if err := builder.WriteString(" ) "); err != nil {
				return err
			}
		}

		if err := builder.WriteString(f.Name); err != nil {
			return err
		}

		if err := builder.WriteString("("); err != nil {
			return err
		}

		for i, arg := range f.ArgsIn {
			if i != 0 {
				if err := builder.WriteString(", "); err != nil {
					return err
				}
			}

			if err := arg.Generate(builder); err != nil {
				return err
			}
		}

		if err := builder.WriteString(") "); err != nil {
			return err
		}

		switch len(f.ArgsOut) {
		case 0:
			//Exec nothing
		case 1:
			if err := builder.WriteString(f.ArgsOut[0]); err != nil {
				return err
			}

		default:
			for i, argType := range f.ArgsOut {
				if err := builder.WriteString("("); err != nil {
					return err
				}

				if i != 0 {
					if err := builder.WriteString(", "); err != nil {
						return err
					}

				}

				if err := builder.WriteString(argType); err != nil {
					return err
				}

				if err := builder.WriteString(")"); err != nil {
					return err
				}
			}
		}

		if err := builder.WriteString(" {"); err != nil {
			return err
		}

		blockBuilder := builder.IncIndent("  ")
		if err := blockBuilder.WriteIndentedString("\n"); err != nil {
			return err
		}

		if err := f.Body.Generate(blockBuilder); err != nil {
			return err
		}

		if f.Return != nil {
			if err := f.Return.Generate(builder); err != nil {
				return err
			}
		}

		if err := builder.WriteIndentedString("\n}"); err != nil {
			return err
		}

		return nil

	default:
		return unsupportedOptionUse(builder, f)
	}
}
