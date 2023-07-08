package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"sort"
	"sync"
)

type (
	Stater struct {
		route      *Route
		request    *http.Request
		parameters *RequestParams
		cache      *staterCache
		resource   *view.Resource
	}

	staterCache struct {
		index sync.Map
	}

	staterCacheValue struct {
		once    sync.Once
		newer   func() (*stateUpdater, error)
		err     error
		updater *stateUpdater
	}

	stateUpdater struct {
		params      []*view.Parameter
		paramsIndex view.ParametersIndex
	}
)

func (s *Stater) Into(ctx context.Context, into interface{}) error {
	dstType := reflect.TypeOf(into)
	updater, err := s.getUpdater(ctx, dstType)
	if err != nil {
		return err
	}

	return updater.update(ctx, s.request, s.route, into, s.parameters)
}

func (s *Stater) getUpdater(ctx context.Context, dstType reflect.Type) (*stateUpdater, error) {
	cacheValue := s.cache.get(dstType, func() (*stateUpdater, error) {
		return s.newUpdater(ctx, dstType)
	})

	updater, err := cacheValue.getUpdater()
	if err != nil {
		return nil, err
	}

	return updater, nil
}

func (s *Stater) newUpdater(ctx context.Context, dstType reflect.Type) (*stateUpdater, error) {
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

		if currParam, err := s.resource.ParamByName(parameter.Name); err == nil {
			parameter = currParam.WithAccessors(types.NewAccessor([]*xunsafe.Field{xunsafe.NewField(field)}), nil)
		} else {
			if err = parameter.Init(ctx, nil, s.resource, nil); err != nil {
				return nil, err
			}
		}

		params = append(params, parameter)
	}

	sort.Sort(view.ParametersSlice(params))
	index, err := view.ParametersSlice(params).Index()
	if err != nil {
		return nil, err
	}

	return &stateUpdater{
		params:      params,
		paramsIndex: index,
	}, nil
}

func (u *stateUpdater) update(ctx context.Context, request *http.Request, route *Route, dest interface{}, params *RequestParams) error {
	paramBuilder := newParamStateBuilder(
		*route._caser,
		route.DateFormat,
		NewRequestMetadata(route),
		params,
		newParamsValueCache(),
		u.paramsIndex,
	)

	state := &view.ParamState{
		Values: dest,
	}

	param, err := paramBuilder.buildSelectorParameters(ctx, state, nil, u.params, route.CustomValidation)
	if err != nil {
		return NewParamError("", param.Name, err)
	}

	return nil
}

func (v *staterCacheValue) getUpdater() (*stateUpdater, error) {
	v.once.Do(func() {
		v.updater, v.err = v.newer()
	})

	return v.updater, v.err
}

func (c *staterCache) get(rType reflect.Type, newer func() (*stateUpdater, error)) *staterCacheValue {
	actual, _ := c.index.LoadOrStore(rType, &staterCacheValue{
		newer: newer,
	})

	return actual.(*staterCacheValue)
}
