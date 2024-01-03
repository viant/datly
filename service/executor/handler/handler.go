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
var FactoryType = reflect.TypeOf((*handler.Factory)(nil)).Elem()

type (
	Handler struct {
		Type       string
		Arguments  []string
		InputType  string
		OutputType string
		factory    handler.Factory
		_type      reflect.Type
		caller     reflect.Method
		resource   *view.Resource
	}
)

func (h *Handler) Init(ctx context.Context, resource *view.Resource) (err error) {

	aType := h._type
	if aType == nil {
		h.resource = resource
		aType, err = h.resource.TypeRegistry().Lookup(h.Type)
		if err != nil {
			return fmt.Errorf("couldn't parse Handler type due to %w", err)
		}
	}

	if aType.Implements(FactoryType) {
		factory := types.NewValue(h._type)
		if aFactory, ok := factory.(handler.Factory); ok {
			h.factory = aFactory
			return nil
		}
	}

	if _, ok := aType.MethodByName("Exec"); !ok {
		aType = reflect.PtrTo(aType)
	}

	h._type = aType
	if !h._type.Implements(Type) {
		return fmt.Errorf("handler %v has to implement %v", h._type.String(), Type.String())
	}
	method, _ := h._type.MethodByName("Exec")
	h.caller = method
	return nil
}

func (h *Handler) Call(ctx context.Context, session handler.Session) (interface{}, error) {
	var aHandler handler.Handler
	var err error
	if h.factory != nil {
		if aHandler, err = h.factory.New(ctx, h.Arguments...); err != nil {
			return nil, fmt.Errorf("failed to create handler: %w", err)
		}
	}
	if aHandler == nil {
		var ok bool
		if value := types.NewValue(h._type); value != nil {
			if aHandler, ok = value.(handler.Handler); !ok {
				return nil, fmt.Errorf("expected handler to implement %T", aHandler)
			}
		}
	}
	asHandler, ok := aHandler.(handler.Handler)
	if !ok {
		return nil, fmt.Errorf("expected handler to implement %T", asHandler)
	}
	return asHandler.Exec(ctx, session)
}

func NewHandler(handler handler.Handler) *Handler {
	rType := reflect.TypeOf(handler)
	return &Handler{Type: rType.Name(), _type: rType}
}
