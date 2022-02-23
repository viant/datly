package sql

type Node interface {
	Validate(allowed map[string]Kind) error
}
