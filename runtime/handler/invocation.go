package handler

import (
	"context"
	"reflect"

	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

// Invocation is the canonical input, DI, and response surface passed to every
// runtime handler flavor after shared binding and input initialization.
type Invocation struct {
	Input    any
	Binder   xhandler.Binder
	Response xresponse.Writer
	// Snapshot is the opaque pre-initialization capture result. It is also
	// available to outcome finalizers when initialization failed before Execute.
	Snapshot any
}

type Handler interface {
	Execute(ctx context.Context, invocation Invocation) (any, error)
}

// InputCapturer is an opt-in pre-initialization adapter. The engine treats its
// result as opaque; generated/custom handlers own capture and interpretation.
type InputCapturer interface {
	CaptureInput(context.Context, any) (any, error)
}

// TypedHandler exposes the contract types of handlers assembled from Go
// shapes. Registration can use these without reflecting over handler methods.
type TypedHandler interface {
	Handler
	InputType() reflect.Type
	OutputType() reflect.Type
}

type HandlerFunc func(ctx context.Context, invocation Invocation) (any, error)

func (f HandlerFunc) Execute(ctx context.Context, invocation Invocation) (any, error) {
	return f(ctx, invocation)
}
