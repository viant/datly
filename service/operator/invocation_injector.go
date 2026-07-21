package operator

import (
	"context"

	xstate "github.com/viant/xdatly/handler/state"
)

type invocationInjector struct {
	ctx      context.Context
	delegate xstate.Injector
}

func (i *invocationInjector) Into(_ context.Context, value interface{}, options ...xstate.Option) error {
	return i.delegate.Into(i.ctx, value, options...)
}

func (i *invocationInjector) Bind(_ context.Context, value interface{}, options ...xstate.Option) error {
	return i.delegate.Bind(i.ctx, value, options...)
}

func (i *invocationInjector) Value(_ context.Context, key string) (interface{}, bool, error) {
	return i.delegate.Value(i.ctx, key)
}

func (i *invocationInjector) ValuesOf(_ context.Context, value interface{}) (map[string]interface{}, error) {
	return i.delegate.ValuesOf(i.ctx, value)
}
