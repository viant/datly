package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/xdatly/handler"
	"reflect"
	"strings"
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
	handlerTypeName := h.HandlerType
	pkg := ""
	if index := strings.LastIndex(handlerTypeName, "."); index != -1 {
		pkg = handlerTypeName[:index]
		handlerTypeName = handlerTypeName[index+1:]
	}
	handlerType, err := h.resource.LookupType("", pkg, handlerTypeName)
	//	handlerType, err := types.GetOrParseType(h.resource.LookupType, h.HandlerType)
	if err != nil {
		return fmt.Errorf("couldn't parse Handler type due to %w", err)
	}

	if _, ok := handlerType.MethodByName("Exec"); !ok {
		handlerType = reflect.PtrTo(handlerType)
	}
	h._handlerType = handlerType
	if !h._handlerType.Implements(HandlerType) {
		return fmt.Errorf("handler %v has to implement %v", h._handlerType.String(), HandlerType.String())
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
