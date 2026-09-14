package custom

import (
	"context"
	"fmt"
	"reflect"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type funcHandler[I any, O any] struct {
	fn xhandler.Func[I, O]
}

// NewFunc adapts a simple pure Go function. Handlers that need DI or response
// control should use New with xdatly/handler.Contract instead.
func NewFunc[I any, O any](fn xhandler.Func[I, O]) rhandler.TypedHandler {
	return &funcHandler[I, O]{fn: fn}
}

func (h *funcHandler[I, O]) InputType() reflect.Type {
	return reflect.TypeFor[I]()
}

func (h *funcHandler[I, O]) OutputType() reflect.Type {
	return reflect.TypeFor[O]()
}

func (h *funcHandler[I, O]) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	if h == nil || h.fn == nil {
		return nil, fmt.Errorf("custom handler function is required")
	}
	input, ok := invocation.Input.(*I)
	if !ok || input == nil {
		return nil, fmt.Errorf("custom handler input must be *%s, got %T", reflect.TypeFor[I](), invocation.Input)
	}
	return h.fn(ctx, input)
}
