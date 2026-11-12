package provider

import (
	"context"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/invocationbinder"
	"github.com/viant/structology"
	"reflect"
)

// InvocationBinder exposes the binder active in the current predicate evaluation.
// It is not cached: nested evaluations can carry another binder in the same
// composed provider tree and must retain their own canonical input/selector scope.
func InvocationBinder() locator.Provider { return invocationBinderProvider{} }

type invocationBinderProvider struct{}

func (invocationBinderProvider) Kind() string                                { return invocationbinder.Kind }
func (invocationBinderProvider) Priority() int                               { return 0 }
func (invocationBinderProvider) DefaultCacheable() bool                      { return false }
func (p invocationBinderProvider) Locate(*structology.State) locator.Locator { return p }
func (invocationBinderProvider) Value(ctx context.Context, _ reflect.Type, name string) (any, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if name != "" {
		return nil, false, nil
	}
	value, ok := invocationbinder.From(ctx)
	return value, ok, nil
}
