package sql

type Parentheses struct {
	P Node
}

func (p *Parentheses) Validate(allowed map[string]int) error {
	return p.P.Validate(allowed)
}
