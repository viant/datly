package ast

import "fmt"

func (s *Foreach) Generate(builder *Builder) (err error) {
	switch builder.Lang {
	case "dsql":
		if err = builder.WriteIndentedString("\n#foreach("); err != nil {
			return err
		}
		if err = s.Value.Generate(builder); err != nil {
			return err
		}
		if err = builder.WriteString(" in "); err != nil {
			return err
		}
		if err = s.Set.Generate(builder); err != nil {
			return err
		}
		if err = builder.WriteString(")"); err != nil {
			return err
		}

		bodyBuilder := builder.IncIndent("  ")
		if err = s.Body.Generate(bodyBuilder); err != nil {
			return err
		}
		if err = builder.WriteIndentedString("\n#end"); err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("unsupported option %T %v\n", s, builder.Lang)
}
