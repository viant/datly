package sql

type Unary struct {
	X        Node
	Operator string
}

func (u *Unary) Validate(allowed map[string]int) error {
	return u.X.Validate(allowed)
}
