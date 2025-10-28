package state

import (
	"context"

	"github.com/viant/xdatly/handler/state"
)

// Initializer is an interface that should be implemented by any type that needs to be initialized
type Initializer interface {
	Init(ctx context.Context) error
}

// Finalizer is an interface that should be implemented by any type that needs to be finalized
type Finalizer interface {
	Finalize(ctx context.Context) error
}

type InjectorFinalizer interface {
	Finalize(ctx context.Context, injector state.Injector) error
}
