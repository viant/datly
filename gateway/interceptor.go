package gateway

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/template/expand"
	"github.com/viant/xreflect"
	"net/http"
	"reflect"
)

var contextType = reflect.TypeOf(InterceptorContext{})

type (
	RouteInterceptor struct {
		URL         string
		evaluator   *expand.Evaluator
		template    string
		typeLookup  xreflect.TypeLookupFn
		contextType *expand.CustomContext
	}

	RouterInterceptors map[string]*RouteInterceptor
)

func (i RouterInterceptors) AsSlice() []*RouteInterceptor {
	result := make([]*RouteInterceptor, 0, len(i))
	for _, interceptor := range i {
		result = append(result, interceptor)
	}

	return result
}

func (i RouterInterceptors) Copy() RouterInterceptors {
	result := RouterInterceptors{}

	for s, interceptor := range i {
		result[s] = interceptor
	}

	return result
}

func NewInterceptorFromURL(ctx context.Context, fs afs.Service, URL string, fn xreflect.TypeLookupFn) (*RouteInterceptor, error) {
	veltyContent, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}

	result := &RouteInterceptor{
		URL:        URL,
		template:   string(veltyContent),
		typeLookup: fn,
	}

	return result, result.init()
}

func (i *RouteInterceptor) init() error {
	i.contextType = i.newContext(InterceptorContext{})

	evaluator, err := expand.NewEvaluator(nil, nil, nil, i.template, i.typeLookup, i.contextType)
	if err != nil {
		return err
	}

	i.evaluator = evaluator
	return nil
}

func (i *RouteInterceptor) newContext(ctx InterceptorContext) *expand.CustomContext {
	return &expand.CustomContext{
		Type:  contextType,
		Value: ctx,
	}
}

func (i *RouteInterceptor) Evaluate(request *http.Request) (*IntereceptorState, error) {
	req := httputils.RequestOf(request)
	ctx := &InterceptorContext{
		Request: req,
		Router:  &RouteHandler{request: request},
	}
	state, err := i.evaluator.Evaluate(nil, nil, nil, nil, nil, i.newContext(*ctx))
	return &IntereceptorState{
		ExpandState: state,
		Context:     ctx,
	}, err
}
