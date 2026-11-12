// Package invocationbinder carries the active binder during dependency binding.
// It creates neither an injector nor an independent resolution scope.
package invocationbinder

import (
	"context"
	xhandler "github.com/viant/xdatly/handler"
)

const Kind = "binder"

type key struct{}

func With(ctx context.Context, binder xhandler.Binder) context.Context {
	return context.WithValue(ctx, key{}, binder)
}

func From(ctx context.Context) (xhandler.Binder, bool) {
	if ctx == nil {
		return nil, false
	}
	binder, ok := ctx.Value(key{}).(xhandler.Binder)
	return binder, ok && binder != nil
}
