package velty

type expression interface {
	render(*renderer) error
}

type statement interface {
	render(*renderer, int) error
}

type selector string
type stringLiteral string
type booleanLiteral bool

type call struct {
	receiver expression
	method   string
	args     []expression
}

type binary struct {
	left  expression
	op    string
	right expression
}

type callStatement struct {
	call       call
	terminated bool
}

type assignment struct {
	target expression
	value  expression
}

type forEach struct {
	item string
	set  expression
	body block
}

type conditional struct {
	condition   expression
	compareTrue bool
	thenBlock   block
	elseBlock   *block
}

type block struct {
	statements []statement
}

func (b *block) append(value statement) {
	if b != nil && value != nil {
		b.statements = append(b.statements, value)
	}
}
