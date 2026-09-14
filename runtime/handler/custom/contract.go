// Package custom adapts typed user-defined Go handlers to the shared runtime
// handler contract. It owns no binding, routing, SQL, or transaction behavior.
package custom

import (
	"context"
	"fmt"
	"reflect"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type contractHandler[I any, O any] struct {
	contract xhandler.Contract[I, O]
}

// New adapts the public binder-aware custom Go contract to the unified runtime
// handler. The engine has already bound and initialized input before Execute.
func New[I any, O any](contract xhandler.Contract[I, O]) rhandler.TypedHandler {
	return &contractHandler[I, O]{contract: contract}
}

func (h *contractHandler[I, O]) InputType() reflect.Type {
	return reflect.TypeFor[I]()
}

func (h *contractHandler[I, O]) OutputType() reflect.Type {
	return reflect.TypeFor[O]()
}

func (h *contractHandler[I, O]) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	if h == nil || h.contract == nil {
		return nil, fmt.Errorf("custom handler contract is required")
	}
	input, ok := invocation.Input.(*I)
	if !ok || input == nil {
		return nil, fmt.Errorf("custom handler input must be *%s, got %T", reflect.TypeFor[I](), invocation.Input)
	}
	output := new(O)
	session := newSession(invocation.Binder, invocation.Response)
	ctx = xhandler.WithSession(ctx, session)
	return output, h.contract.Exec(ctx, session, input, output)
}
