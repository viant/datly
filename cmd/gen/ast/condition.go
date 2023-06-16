package ast

import "fmt"

func (s *Condition) Generate(builder *Builder) (err error) {
	switch builder.Lang {
	case "dsql":
		if err = builder.WriteIndentedString("\n#if("); err != nil {
			return err
		}
		if err = s.If.Generate(builder); err != nil {
			return err
		}
		if err = builder.WriteString(")"); err != nil {
			return err
		}
		bodyBuilder := builder.IncIndent("  ")
		if err = s.IFBlock.Generate(bodyBuilder); err != nil {
			return err
		}
		for _, item := range s.ElseIfBlocks {
			if err = builder.WriteIndentedString("\n#elseif("); err != nil {
				return err
			}
			if err = item.If.Generate(builder); err != nil {
				return err
			}
			if err = builder.WriteString(")"); err != nil {
				return err
			}
			if err = item.Block.Generate(bodyBuilder); err != nil {
				return err
			}
		}
		if s.ElseBlock != nil {
			if err = builder.WriteIndentedString("\n#else"); err != nil {
				return err
			}
			if err = s.ElseBlock.Generate(bodyBuilder); err != nil {
				return err
			}
		}
		if err = builder.WriteIndentedString("\n#end"); err != nil {
			return err
		}
		return nil

	}

	return fmt.Errorf("unsupported option %T %v\n", s, builder.Lang)
}
