package ast

import "fmt"

type (
	Condition struct {
		If           Expression
		IFBlock      Block
		ElseIfBlocks []*ConditionalBlock
		ElseBlock    Block
	}

	ConditionalBlock struct {
		If    Expression
		Block Block
	}
)

func (s *Condition) Generate(builder *Builder) (err error) {
	switch builder.Lang {
	case LangDSQL:
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

	case LangGO:
		if err = builder.WriteIndentedString("\nif "); err != nil {
			return err
		}

		if err = s.If.Generate(builder); err != nil {
			return err
		}

		if err = builder.WriteString(" {"); err != nil {
			return err
		}

		bodyBlockBuilder := builder.IncIndent("  ")
		if err = bodyBlockBuilder.WriteIndentedString("\n"); err != nil {
			return err
		}

		if err = s.IFBlock.Generate(bodyBlockBuilder); err != nil {
			return err
		}

		if err = builder.WriteIndentedString("\n}"); err != nil {
			return err
		}

		for _, block := range s.ElseIfBlocks {
			if err = builder.WriteString(" else if "); err != nil {
				return err
			}

			if err = block.If.Generate(builder); err != nil {
				return err
			}

			if err = builder.WriteString(" { "); err != nil {
				return err
			}

			if err = bodyBlockBuilder.WriteIndentedString("\n"); err != nil {
				return err
			}

			if err = block.Block.Generate(bodyBlockBuilder); err != nil {
				return err
			}

			if err = builder.WriteIndentedString("\n} "); err != nil {
				return err
			}
		}

		return nil
	}

	return fmt.Errorf("unsupported option %T %v\n", s, builder.Lang)
}

func NewCondition(ifExpr Expression, ifBlock, elseBlock Block) *Condition {
	return &Condition{If: ifExpr, IFBlock: ifBlock, ElseBlock: elseBlock}
}
