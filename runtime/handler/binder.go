package handler

import (
	"context"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	xhandler "github.com/viant/xdatly/handler"
)

// Binder is the xdatly handler facade over one Bindly invocation scope.
type Binder struct {
	injector *bindly.Injector
	input    any
}

func NewBinder(injector *bindly.Injector, input any) *Binder {
	return &Binder{injector: injector, input: input}
}

func (b *Binder) Bind(ctx context.Context, target any, options ...bindly.BindOption) error {
	if len(options) == 0 {
		return b.injector.Bind(ctx, target, bindly.WithSource(b.input))
	}
	// Keep the canonical input as the default source. Explicit options have
	// the same last-option-wins semantics as the native Bindly call.
	return b.injector.Bind(ctx, target, append([]bindly.BindOption{bindly.WithSource(b.input)}, options...)...)
}

func (b *Binder) Lookup(ctx context.Context, key xhandler.ValueKey) (any, bool, error) {
	state := bindly.WithState[any](b.injector, b.input)
	return state.Value(ctx, &bindstate.Location{Kind: string(key)})
}
