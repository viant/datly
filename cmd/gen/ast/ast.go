package ast

import (
	"fmt"
)

type (
	Node interface {
		Generate(builder *Builder) error
	}
	Statement interface {
		Node
	}

	Expression interface {
		Node
	} //can be BinaryExpr or CallExpr or Selector Expr

	Block []Statement
	Ident struct {
		Name string
	}

	Foreach struct {
		Value *Ident
		Set   *Ident
		Body  *Block
	}

	Assign struct {
		Holder     Expression
		Expression Expression
	}

	CallExpr struct {
		Holder Ident
		Args   []Expression
	}

	VoidExpression struct {
		Expression
	}

	SelectorExpr struct {
		Ident
		X *SelectorExpr
	}

	BinaryExpr struct {
		X  Expression
		Op string
		Y  Expression
	}

	LiteralExpr struct {
		Literal string
	}

	ConditionalBlock struct {
		If    Expression
		Block Block
	}

	Condition struct {
		If           Expression
		IFBlock      Block
		ElseIfBlocks []*ConditionalBlock
		ElseBlock    Block
	}

	Options struct {
		Lang string
	}
)

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

func (b Block) Generate(builder *Builder) error {
	for _, stmt := range b {
		if err := stmt.Generate(builder); err != nil {
			return err
		}
	}
	return nil
}

func (e Ident) Generate(builder *Builder) (err error) {
	if builder.Lang == "dsql" {
		return builder.WriteString("$" + e.Name)
	}
	return builder.WriteString(e.Name)
}
