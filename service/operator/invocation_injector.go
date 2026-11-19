package operator

import (
	"context"

	"github.com/viant/datly/service/executor/uow"
	xstate "github.com/viant/xdatly/handler/state"
)

type invocationInjector struct {
	ctx      context.Context
	name     string
	delegate xstate.Injector
}

func (i *invocationInjector) Into(_ context.Context, value interface{}, options ...xstate.Option) error {
	return i.invoke(func(ctx context.Context) error {
		return i.delegate.Into(ctx, value, options...)
	})
}

func (i *invocationInjector) Bind(_ context.Context, value interface{}, options ...xstate.Option) error {
	return i.invoke(func(ctx context.Context) error {
		return i.delegate.Bind(ctx, value, options...)
	})
}

func (i *invocationInjector) Value(_ context.Context, key string) (interface{}, bool, error) {
	var value interface{}
	var ok bool
	err := i.invoke(func(ctx context.Context) error {
		var err error
		value, ok, err = i.delegate.Value(ctx, key)
		return err
	})
	return value, ok, err
}

func (i *invocationInjector) ValuesOf(_ context.Context, value interface{}) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := i.invoke(func(ctx context.Context) error {
		var err error
		result, err = i.delegate.ValuesOf(ctx, value)
		return err
	})
	return result, err
}

func (i *invocationInjector) invoke(fn func(context.Context) error) error {
	ctx := uow.PrepareChild(i.ctx, uow.RelationImperative, "")
	ctx, _, frame, _, err := uow.Enter(ctx, i.name)
	if err != nil {
		return err
	}
	defer frame.Seal()
	return fn(ctx)
}
