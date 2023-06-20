package ast

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
		Body  Block
	}

	Assign struct {
		Holder     Expression
		Expression Expression
	}

	CallExpr struct {
		Holder Expression
		Name   string
		Args   []Expression
	}

	StatementExpression struct {
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

func (b *Block) Append(statement Statement) {
	*b = append(*b, statement)
}

func (b *Block) AppendEmptyLine() {
	b.Append(NewStatementExpression(NewLiteral("")))
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
