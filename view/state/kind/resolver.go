package kind

import (
	"fmt"
	"github.com/viant/datly/view/state"
	"sync"
)

type Resolvers struct {
	sync.RWMutex
	byKind map[state.Kind]Resolver
	parent *Resolvers
}

func (r *Resolvers) Lookup(kind state.Kind) (Resolver, error) {
	resolver, ok := r.byKind[kind]
	if ok {
		return resolver, nil
	}
	if r.parent == nil {
		return nil, fmt.Errorf("failed to lookup resolver for kind: %v", kind)
	}
	return r.parent.Lookup(kind)
}

type Resolver interface {
	Names() []string
	Value(name string) (interface{}, bool, error)
}
