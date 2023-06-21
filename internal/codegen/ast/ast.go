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
		WithState bool
		Name      string
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
		Receiver Expression
		Name     string
		Args     []Expression
	}

	MapExpr struct {
		Map Expression
		Key Expression
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
)

func (m *MapExpr) Generate(builder *Builder) error {
	if err := m.Map.Generate(builder); err != nil {
		return err
	}

	if err := builder.WriteString("["); err != nil {
		return err
	}

	if err := m.Key.Generate(builder); err != nil {
		return err
	}

	if err := builder.WriteString("]"); err != nil {
		return err
	}

	return nil
}

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
	identName := e.Name
	if e.WithState && builder.StateName != "" {
		identName = identName + "." + builder.StateName
	}

	builder.State.DeclareVariable(identName)

	if builder.Lang == LangDSQL {
		return builder.WriteString("$" + identName)
	}
	return builder.WriteString(identName)
}
