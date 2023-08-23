package ast

import (
	"fmt"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/toolbox/format"
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
	} //can be BinaryExpr or CallExpr or QuerySelector Expr

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
		Holder       Expression
		ExtraHolders []Expression
		Expression   Expression
	}

	CallExpr struct {
		Terminator bool
		Receiver   Expression
		Name       string
		Args       []Expression
	}

	MapExpr struct {
		Map Expression
		Key Expression
	}

	StatementExpression struct {
		Expression
	}

	TerminatorExpression struct {
		X Expression
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

	ReturnExpr struct {
		X Expression
	}
)

func NewReturnExpr(expr Expression) *ReturnExpr {
	return &ReturnExpr{X: expr}
}
func (r *ReturnExpr) Generate(builder *Builder) error {
	switch builder.Lang {
	case LangGO:
		if err := builder.WriteIndentedString("\nreturn "); err != nil {
			return err
		}

		return r.X.Generate(builder)
	}

	return fmt.Errorf("unsupported %T with lang %v", r, builder.Lang)
}

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
	if builder.WithoutBusinessLogic {
		return nil
	}
	for _, stmt := range b {
		if err := stmt.Generate(builder); err != nil {
			return err
		}
	}
	return nil
}

func (e Ident) Generate(builder *Builder) (err error) {
	identName := e.Name
	if builder.WithLowerCaseIdent {
		upperCamel, _ := formatter.UpperCamel.Caser()
		identName = upperCamel.Format(identName, format.CaseLowerCamel)
	}
	if e.WithState && builder.StateName != "" {
		identName = identName + "." + builder.StateName
	}

	builder.State.DeclareVariable(identName)

	if builder.Lang == LangVelty {
		return builder.WriteString("$" + identName)
	}
	return builder.WriteString(identName)
}

func (b TerminatorExpression) Generate(builder *Builder) error {
	if err := b.X.Generate(builder); err != nil {
		return err
	}
	if builder.Lang == LangVelty {
		return builder.WriteByte(';')
	}
	return nil
}

func NewTerminatorExpression(x Expression) *TerminatorExpression {
	return &TerminatorExpression{X: x}
}
