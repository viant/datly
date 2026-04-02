package state

import (
	"context"

	"github.com/viant/xdatly/handler/http"
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

// FinaliserWithError is an error-aware finalizer that receives an error from previous steps.
type FinalizerWithError interface {
	Finalize(ctx context.Context, err error) error
}

type InjectorFinalizer interface {
	Finalize(ctx context.Context, getInjector func(ctx context.Context, path http.Route) (state.Injector, error)) error
}
