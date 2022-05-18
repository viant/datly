package sql

type Kind int

const (
	Null Kind = iota - 1
	Unspecified
	Int
	String
	Bool
	Float
)
