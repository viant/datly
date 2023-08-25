package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	vsession "github.com/viant/datly/view/session"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
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
		params      []*state.Parameter
		paramsIndex state.NamedParameters
	}
)

func (s *Stater) Into(ctx context.Context, into interface{}) error {
	dstType := reflect.TypeOf(into)
	updater, err := s.getUpdater(ctx, dstType)
	if err != nil {
		return err
	}
	return updater.update(ctx, s.request, s.route, into)
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
	params := make([]*state.Parameter, 0, fieldLen)
	aResource := view.NewResourcelet(s.resource, nil)

	for i := 0; i < fieldLen; i++ {
		field := elemType.Field(i)
		parameter, err := BuildParameter(field)
		if err != nil {
			return nil, err
		}

		if currParam, err := s.resource.LookupParameter(parameter.Name); err == nil {
			parameter = currParam.Clone()
		}
		if err = parameter.Init(ctx, aResource); err != nil {
			return nil, err
		}

		params = append(params, parameter)
	}

	sort.Sort(state.Parameters(params))

	return &stateUpdater{
		params:      params,
		paramsIndex: state.Parameters(params).Index(),
	}, nil
}

func (u *stateUpdater) update(ctx context.Context, request *http.Request, route *Route, dest interface{}) error {
	stateType := structology.NewStateType(reflect.TypeOf(dest))
	viewState := stateType.WithValue(dest)
	sessionState := vsession.New(route.View, vsession.WithLocatorOptions(route.LocatorOptions(request)...))
	options := sessionState.ViewOptions(route.View)
	return sessionState.SetState(ctx, route.View.Template.Parameters, viewState, options.Indirect(true))
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
