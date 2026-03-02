package handler

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

var Type = reflect.TypeOf((*handler.Handler)(nil)).Elem()
var FactoryType = reflect.TypeOf((*handler.Factory)(nil)).Elem()

type (
	Handler struct {
		Type       string
		Arguments  []string
		InputType  string
		OutputType string
		MessageBus string
		ProxyURL   string
		Output     reflect.Type
		factory    handler.Factory
		handler    handler.Handler
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
			if aType = lookupByPackagePathAlias(h.resource.TypeRegistry().Lookup, h.Type); aType == nil {
				return fmt.Errorf("couldn't parse Handler type due to %w", err)
			}
		}
	}
	if aType.Kind() != reflect.Ptr {
		aType = reflect.PtrTo(aType)
	}

	if aType.Implements(FactoryType) {
		factory := types.NewValue(aType)
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
	if h.handler != nil {
		return h.handler.Exec(ctx, session)
	}

	var aHandler handler.Handler
	if h.factory != nil {
		options, err := h.buildFactoryOptions()
		if err != nil {
			return nil, err
		}
		if aHandler, err = h.factory.New(ctx, options...); err != nil {
			return nil, fmt.Errorf("failed to create handler: %w", err)
		}
	}
	if aHandler == nil {
		var ok bool
		if value := types.NewValue(h._type); value != nil {
			if aHandler, ok = value.(handler.Handler); !ok {
				return nil, fmt.Errorf("expected handler to implement %T", aHandler)
			}
			if initializer, ok := value.(state.Initializer); ok {
				if err := initializer.Init(ctx); err != nil {
					return nil, fmt.Errorf("failed to initialize handler %T, %w", aHandler, err)
				}
			}

		}
	}
	var ok bool
	aHandler, ok = aHandler.(handler.Handler)
	if !ok {
		return nil, fmt.Errorf("expected handler to implement %T", aHandler)
	}

	h.handler = aHandler
	return aHandler.Exec(ctx, session)
}

func (h *Handler) buildFactoryOptions() ([]handler.Option, error) {
	lookupType := h.resource.LookupType()
	var options = []handler.Option{handler.WithArguments(h.Arguments), handler.WithLookupType(func(name string) (reflect.Type, error) {
		return lookupType(name)
	})}
	if h.InputType != "" {
		inputType, err := lookupType(h.InputType)
		if err != nil {
			return nil, fmt.Errorf("failed to lookup handler input type: %w", err)
		}
		options = append(options, handler.WithInputType(inputType))
	}
	if h.Output != nil {
		options = append(options, handler.WithOutputType(h.Output))
	}
	return options, nil
}

func NewHandler(handler handler.Handler) *Handler {
	rType := reflect.TypeOf(handler)
	return &Handler{Type: rType.Name(), _type: rType}
}

func lookupByPackagePathAlias(lookup xreflect.LookupType, typeName string) reflect.Type {
	typeName = strings.TrimSpace(typeName)
	index := strings.LastIndex(typeName, ".")
	if index == -1 || index == len(typeName)-1 {
		return nil
	}
	pkgPath := typeName[:index]
	name := typeName[index+1:]
	if !strings.Contains(pkgPath, "/") {
		return nil
	}
	segments := strings.Split(pkgPath, "/")
	var candidates []string
	if len(segments) >= 2 {
		candidates = append(candidates, strings.Join(segments[len(segments)-2:], "/"))
	}
	candidates = append(candidates, segments[len(segments)-1])
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if rType, err := lookup(name, xreflect.WithPackage(candidate)); err == nil && rType != nil {
			return rType
		}
	}
	if rType := xunsafe.LookupType(pkgPath + "/" + name); rType != nil {
		return rType
	}
	return nil
}
