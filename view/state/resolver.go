package state

type Resolvers struct {
	byKind map[int]Resolver
	sideR  []*Resolvers
}

type Resolver interface {
	Value(name string) (interface{}, bool, error)
}
