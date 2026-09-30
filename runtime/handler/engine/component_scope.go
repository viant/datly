package engine

import (
	"context"
	"errors"
)

var ErrTransactionConflict = errors.New("component transaction conflicts with the root database unit")
var ErrUnknownDatabaseIdentity = errors.New("nested component database identity is unknown")

type ComponentRelation string

const (
	ComponentBinding    ComponentRelation = "binding"
	ComponentImperative ComponentRelation = "imperative"
)

type componentScope struct {
	relation ComponentRelation
	order    string
}

type componentScopeKey struct{}

// PrepareComponent marks the relationship consumed by the next nested engine
// invocation. The marker remains internal to Datly's dispatcher flow.
func PrepareComponent(ctx context.Context, relation ComponentRelation, order string) context.Context {
	return context.WithValue(ctx, componentScopeKey{}, componentScope{relation: relation, order: order})
}

func componentFromContext(ctx context.Context) componentScope {
	value, _ := ctx.Value(componentScopeKey{}).(componentScope)
	if value.relation == "" {
		value.relation = ComponentBinding
	}
	return value
}

// IsImperativeComponent reports whether this handler was invoked as an
// explicit child call. A child writer may flush its own buffered prefix so
// later sibling readers observe it inside the root-owned transaction.
func IsImperativeComponent(ctx context.Context) bool {
	scope, ok := ctx.Value(componentScopeKey{}).(componentScope)
	return ok && scope.relation == ComponentImperative
}
