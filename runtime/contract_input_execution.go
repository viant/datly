package runtime

import (
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/x/shape"
)

// preparedInputExecutionPlan contains only immutable registration metadata.
// Retention belongs to contract.Plan's existing bounded variant entry. The
// operation's binding handoff carries a reference, never cached policy authority.
type preparedInputExecutionPlan struct {
	original       *registry.RouteInputContract
	representation *shape.Representation
	route          *registry.RouteInputContract
}

func prepareInputExecution(original *registry.RouteInputContract) func(*shape.Representation) (any, error) {
	return func(metadata *shape.Representation) (any, error) {
		route, err := original.WithInputRepresentation(metadata)
		if err != nil {
			return nil, err
		}
		return &preparedInputExecutionPlan{original: original, representation: metadata, route: route}, nil
	}
}

func (b *contractBindings) retainPreparedInput(prepared *preparedInputExecutionPlan) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	if b.preparedInputs == nil {
		b.preparedInputs = make(map[*registry.RouteInputContract]*preparedInputExecutionPlan)
	}
	b.preparedInputs[prepared.original] = prepared
}

func (b *contractBindings) preparedInput(original *registry.RouteInputContract, metadata *shape.Representation) *registry.RouteInputContract {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	prepared := b.preparedInputs[original]
	if prepared == nil || prepared.representation != metadata {
		return nil
	}
	return prepared.route
}
