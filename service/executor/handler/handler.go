package handler

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/xdatly/handler"
	"reflect"
)

var Type = reflect.TypeOf((*handler.Handler)(nil)).Elem()

type (
	Handler struct {
		HandlerType string
		StateType   string

		_handlerType reflect.Type
		_stateType   reflect.Type
		caller       reflect.Method
		resource     *view.Resource
	}
)

func (h *Handler) Init(ctx context.Context, resource *view.Resource) error {
	h.resource = resource
	handlerType, err := h.resource.TypeRegistry().Lookup(h.HandlerType)
	if err != nil {
		return fmt.Errorf("couldn't parse Handler type due to %w", err)
	}

	if _, ok := handlerType.MethodByName("Exec"); !ok {
		handlerType = reflect.PtrTo(handlerType)
	}
	h._handlerType = handlerType
	if !h._handlerType.Implements(Type) {
		return fmt.Errorf("handler %v has to implement %v", h._handlerType.String(), Type.String())
	}

	method, _ := h._handlerType.MethodByName("Exec")
	h.caller = method

	return nil
}

func (h *Handler) Call(ctx context.Context, session handler.Session) (interface{}, error) {
	aHandler := types.NewValue(h._handlerType)
	asHandler, ok := aHandler.(handler.Handler)
	if !ok {
		return nil, fmt.Errorf("expected handler to implement %T", asHandler)
	}

	return asHandler.Exec(ctx, session)
}
