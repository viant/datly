package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"sort"
	"sync"
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
		cache        *handlerCache
	}

	handlerCache struct {
		index sync.Map
	}

	handlerCacheValue struct {
		once    sync.Once
		newer   func() (*stateUpdater, error)
		err     error
		updater *stateUpdater
	}

	stateUpdater struct {
		params []*view.Parameter
	}

	Stater struct {
		handler *Handler
		route   *Route
		request *http.Request
	}
)

func (h *Handler) Init(ctx context.Context, resource *view.Resource) error {
	h.resource = resource
	h.cache = &handlerCache{
		index: sync.Map{},
	}

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

	if h.StateType != "" {
		stateType, err := types.GetOrParseType(h.resource.LookupType, h.StateType)
		if err != nil {
			return err
		}

		h._stateType = stateType
		_, err = h.getUpdater(ctx, h._stateType)
		if err != nil {
			return err
		}
	}

	return nil
}

func (u *stateUpdater) update(ctx context.Context, request *http.Request, route *Route, dest interface{}) error {
	parameters, err := NewRequestParameters(request, route)
	if err != nil {
		return err
	}

	paramBuilder := newParamStateBuilder(
		*route._caser,
		route.DateFormat,
		NewRequestMetadata(route),
		parameters,
		newParamsValueCache(),
	)

	state := &view.ParamState{
		Values: dest,
	}

	param, err := paramBuilder.buildSelectorParameters(ctx, state, nil, u.params)
	if err != nil {
		return NewParamError("", param.Name, err)
	}

	return nil
}

func (v *handlerCacheValue) getUpdater() (*stateUpdater, error) {
	v.once.Do(func() {
		v.updater, v.err = v.newer()
	})

	return v.updater, v.err
}

func (c *handlerCache) get(rType reflect.Type, newer func() (*stateUpdater, error)) *handlerCacheValue {
	actual, _ := c.index.LoadOrStore(rType, &handlerCacheValue{
		newer: newer,
	})

	return actual.(*handlerCacheValue)
}

func (h *Handler) UpdateState(ctx context.Context, request *http.Request, route *Route, dest interface{}) error {
	dstType := reflect.TypeOf(dest)
	updater, err := h.getUpdater(ctx, dstType)
	if err != nil {
		return err
	}

	return updater.update(ctx, request, route, dest)
}

func (h *Handler) getUpdater(ctx context.Context, dstType reflect.Type) (*stateUpdater, error) {
	cacheValue := h.cache.get(dstType, func() (*stateUpdater, error) {
		return h.newUpdater(ctx, dstType)
	})

	updater, err := cacheValue.getUpdater()
	if err != nil {
		return nil, err
	}

	return updater, nil
}

func (h *Handler) newUpdater(ctx context.Context, dstType reflect.Type) (*stateUpdater, error) {
	elemType := dstType
	if dstType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("state has to be a struct or pointer to struct but was %v", dstType.String())
	}

	fieldLen := elemType.NumField()
	params := make([]*view.Parameter, 0, fieldLen)
	for i := 0; i < fieldLen; i++ {
		field := elemType.Field(i)
		parameter, err := BuildParameter(field)
		if err != nil {
			return nil, err
		}

		if existingParam, err := h.resource.ParamByName(parameter.Name); err == nil {
			parameter = existingParam.WithAccessors(types.NewAccessor([]*xunsafe.Field{xunsafe.NewField(field)}), nil)
		} else {
			if err = parameter.Init(ctx, nil, h.resource, nil); err != nil {
				return nil, err
			}
		}

		params = append(params, parameter)
	}

	sort.Sort(view.ParametersSlice(params))
	return &stateUpdater{
		params: params,
	}, nil
}

func (h *Handler) NewStater(request *http.Request, route *Route) *Stater {
	return &Stater{
		handler: h,
		route:   route,
		request: request,
	}
}

func (h *Handler) Call(values []reflect.Value) (interface{}, error) {
	handlerV := types.NewRValue(h._handlerType)
	values = append([]reflect.Value{handlerV}, values...)
	output := h.caller.Func.Call(values)
	result := output[0].Interface()
	resultErr := output[1].Interface()

	asErr, _ := resultErr.(error)
	return result, asErr
}

func (s *Stater) Into(ctx context.Context, into interface{}) error {
	return s.handler.UpdateState(ctx, s.request, s.route, into)
}
