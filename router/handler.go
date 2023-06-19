package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/xdatly/handler"
	"reflect"
)

var HandlerType = reflect.TypeOf((*handler.Handler)(nil)).Elem()

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

	handlerType, err := types.GetOrParseType(h.resource.LookupType, h.HandlerType)
	if err != nil {
		return fmt.Errorf("couldn't parse Handler type due to %w, err")
	}

	h._handlerType = handlerType
	if !h._handlerType.Implements(HandlerType) {
		return fmt.Errorf("handler has to implement %v", HandlerType.String())
	}

	method, _ := h._handlerType.MethodByName("Exec")
	h.caller = method

	return nil
}

func (h *Handler) Call(ctx context.Context, session handler.Session) (interface{}, error) {
	handlerV := types.NewRValue(h._handlerType)
	values := []reflect.Value{handlerV, reflect.ValueOf(ctx), reflect.ValueOf(session)}
	output := h.caller.Func.Call(values)
	result := output[0].Interface()
	resultErr := output[1].Interface()

	asErr, _ := resultErr.(error)
	return result, asErr
}
