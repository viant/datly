package router

import (
	"context"
	"github.com/viant/afs"
	expand2 "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/xreflect"
	"net/http"
	"path"
	"reflect"
	"strings"
)

var ContextType = reflect.TypeOf(InterceptorContext{})

type (
	RouteInterceptor struct {
		SourceURL   string
		Template    string
		evaluator   *expand2.Evaluator
		lookupType  xreflect.LookupType
		contextType *expand2.Variable
		_url        string
	}

	RouterInterceptors map[string]*RouteInterceptor
	InterceptorContext struct {
		Request *httputils.Request `velty:"names=request"`
		Router  *RouteHandler      `velty:"names=router"`
	}

	IntereceptorState struct {
		ExpandState *expand2.State
		Context     *InterceptorContext
	}
	RouteHandler struct {
		request    *http.Request `velty:"-"`
		redirected bool          `velty:"-"`
		url        string        `velty:"-"`
	}
)

func (r *RouteHandler) RedirectTo(newPath string) string {
	if r.url == "" {
		r.request.URL.Path = newPath
	} else {
		r.request.URL.Path = path.Join(strings.TrimRight(r.url, "/"), strings.TrimLeft(newPath, "/"))
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

func NewInterceptorFromURL(ctx context.Context, fs afs.Service, URL string, fn xreflect.LookupType) (*RouteInterceptor, error) {
	veltyContent, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	tt := string(veltyContent)
	result := &RouteInterceptor{
		SourceURL:  URL,
		Template:   tt,
		lookupType: fn,
	}
	return result, result.init("")
}

func (i *RouteInterceptor) init(URL string) error {
	i._url = URL
	i.contextType = i.newContext(InterceptorContext{})

	evaluator, err := expand2.NewEvaluator(i.Template, expand2.WithTypeLookup(i.lookupType),
		expand2.WithPanicOnError(true),
		expand2.WithCustomContexts(i.contextType))
	if err != nil {
		return err
	}

	i.evaluator = evaluator
	return nil
}

func (i *RouteInterceptor) newContext(ctx InterceptorContext) *expand2.Variable {
	return &expand2.Variable{
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

	state, err := i.evaluator.Evaluate(nil, expand2.WithCustomContext(i.newContext(*ctx)))
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
