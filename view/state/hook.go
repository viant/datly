package state

import "context"

// Initializer is an interface that should be implemented by any type that needs to be initialized
type Initializer interface {
	Init(ctx context.Context) error
}

// Finalizer is an interface that should be implemented by any type that needs to be finalized
type Finalizer interface {
	Finalize(ctx context.Context) error
}
