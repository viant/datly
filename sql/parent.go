package sql

type Parentheses struct {
	P Node
}

func (p *Parentheses) Validate(allowed map[string]Kind) error {
	return p.P.Validate(allowed)
}
