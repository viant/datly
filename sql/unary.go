package sql

type Unary struct {
	X        Node
	Operator string
}

func (u *Unary) Validate(allowed map[string]Kind) error {
	return u.X.Validate(allowed)
}
