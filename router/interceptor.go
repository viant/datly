package router

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/template/expand"
	"github.com/viant/xreflect"
	"net/http"
	"reflect"
	"strings"
)

var ContextType = reflect.TypeOf(InterceptorContext{})

type (
	RouteInterceptor struct {
		SourceURL   string
		Template    string
		evaluator   *expand.Evaluator
		typeLookup  xreflect.TypeLookupFn
		contextType *expand.CustomContext
		_url        string
	}

	RouterInterceptors map[string]*RouteInterceptor
	InterceptorContext struct {
		Request *httputils.Request `velty:"names=request"`
		Router  *RouteHandler      `velty:"names=router"`
	}

	IntereceptorState struct {
		ExpandState *expand.State
		Context     *InterceptorContext
	}
	RouteHandler struct {
		request    *http.Request `velty:"-"`
		redirected bool          `velty:"-"`
		url        string        `velty:"-"`
	}
)

func (r *RouteHandler) RedirectTo(path string) string {
	if r.url == "" {
		r.request.URL.Path = path
	} else {
		r.request.URL.Path = url.Join(strings.TrimRight(r.url, "/"), strings.TrimLeft(path, "/"))
	}

	r.redirected = true
	return ""
}

func (r *RouteHandler) Redirected() bool {
	return r.redirected
}

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
	tt := string(veltyContent)
	result := &RouteInterceptor{
		SourceURL:  URL,
		Template:   tt,
		typeLookup: fn,
	}
	return result, result.init("")
}

func (i *RouteInterceptor) init(URL string) error {
	i._url = URL
	i.contextType = i.newContext(InterceptorContext{})

	evaluator, err := expand.NewEvaluator(nil, nil, nil, i.Template, i.typeLookup, i.contextType)
	if err != nil {
		return err
	}

	i.evaluator = evaluator
	return nil
}

func (i *RouteInterceptor) newContext(ctx InterceptorContext) *expand.CustomContext {
	return &expand.CustomContext{
		Type:  ContextType,
		Value: ctx,
	}
}

func (i *RouteInterceptor) Evaluate(request *http.Request) (*IntereceptorState, error) {
	req, err := httputils.RequestOf(request, i._url)
	if err != nil {
		return nil, err
	}

	ctx := &InterceptorContext{
		Request: req,
		Router: &RouteHandler{
			request: request,
			url:     i._url,
		},
	}

	state, err := i.evaluator.Evaluate(nil, nil, nil, nil, nil, i.newContext(*ctx))
	return &IntereceptorState{
		ExpandState: state,
		Context:     ctx,
	}, err
}

func (i *RouteInterceptor) Intercept(request *http.Request) (bool, error) {
	state, err := i.Evaluate(request)
	if err != nil {
		return false, err
	}

	return state.Context.Router.Redirected(), nil
}
