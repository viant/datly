package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	dhttp "github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/response"
	hstate "github.com/viant/xdatly/handler/state"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

type (
	Httper struct {
		executor *Executor
		resource *view.Resource
	}
)

func (h *Httper) rawRequest(ctx context.Context, opts ...state.Option) (*http.Request, error) {
	aSession, err := h.executor.Session(ctx)
	if err != nil {
		return nil, err
	}
	request, err := aSession.Session.HttpRequest(ctx, nil)
	if err != nil {
		return nil, err
	}
	clonedReq := request.Clone(context.Background())
	return clonedReq, nil
}

func (h *Httper) RequestOf(ctx context.Context, any interface{}) (*http.Request, error) {
	anyType := reflect.TypeOf(any)
	if anyType.Kind() == reflect.Ptr {
		anyType = anyType.Elem()
	}
	aSchema := state.NewSchema(anyType)
	aType, err := state.NewType(state.WithSchema(aSchema))
	if err != nil {
		return nil, err
	}
	if err = aType.Init(state.WithResource(view.NewResources(h.resource, h.executor.view))); err != nil {
		return nil, err
	}
	opts, err := h.buildRequestOptions(ctx, aType.Parameters)
	if err != nil {
		return nil, err
	}
	return h.NewRequest(ctx, opts...)

}

func (h *Httper) NewRequest(ctx context.Context, opts ...hstate.Option) (*http.Request, error) {
	options := hstate.NewOptions(opts...)
	rawRequest, err := h.rawRequest(ctx)
	if err != nil {
		return nil, err
	}
	mergeOptionsIntoRequest(rawRequest, options)
	return rawRequest, nil
}

func (h *Httper) Redirect(ctx context.Context, route *dhttp.Route, request *http.Request) error {
	aSession, err := h.executor.Session(ctx)
	if err != nil {
		return err
	}
	aSession.Session.Redirect = &session.Redirect{
		Route:   route,
		Request: request,
	}
	return nil
}

func (h *Httper) FailWithCode(statusCode int, err error) error {
	return response.NewError(statusCode, err.Error(), response.WithError(err))
}

func (h *Httper) buildRequestOptions(ctx context.Context, params []*state.Parameter) ([]hstate.Option, error) {
	aSession, err := h.executor.Session(ctx)
	if err != nil {
		return nil, err
	}
	var opts []hstate.Option
	for _, parameter := range params {
		switch parameter.In.Kind {
		case state.KindQuery:
			value, has, err := aSession.Session.LookupValue(ctx, parameter, nil)
			if err != nil {
				return nil, err
			}
			if !has || value == nil {
				continue
			}
			switch actual := value.(type) {
			case string:

				if parameter.Schema.Type().Kind() == reflect.Slice {
					opts = append(opts, hstate.WithQueryParameters(parameter.In.Name, strings.Split(actual, ",")))
					continue
				}
				opts = append(opts, hstate.WithQueryParameter(parameter.In.Name, actual))
			case []string:
				opts = append(opts, hstate.WithQueryParameters(parameter.In.Name, actual))
			default:
				continue
			}

		case state.KindRequestBody:
			value, has, err := aSession.Session.LookupValue(ctx, parameter, nil)
			if err != nil {
				return nil, err
			}
			if !has || value == nil {
				continue
			}

			if parameter.In.Name != "" {
				continue
			}
			var data []byte
			switch actual := value.(type) {
			case string:
				data = []byte(actual)
			case []byte:
				data = actual
			default:
				data, err = json.Marshal(actual)
				if err != nil {
					return nil, err
				}
			}
			opts = append(opts, hstate.WithBody(data))

		case state.KindHeader:
			value, has, err := aSession.Session.LookupValue(ctx, parameter, nil)
			if err != nil {
				return nil, err
			}
			if !has || value == nil {
				continue
			}
			switch actual := value.(type) {
			case string:
				opts = append(opts, hstate.WithHeader(parameter.In.Name, actual))
			case []string:
				for _, item := range actual {
					opts = append(opts, hstate.WithHeader(parameter.In.Name, item))
				}
			default:
				continue
			}
		case state.KindPath:
			value, has, err := aSession.Session.LookupValue(ctx, parameter, nil)
			if err != nil {
				return nil, err
			}
			if !has || value == nil {
				continue
			}
			switch actual := value.(type) {
			case string:
				opts = append(opts, hstate.WithPathParameter(parameter.In.Name, actual))
			default:
				continue
			}
		}
	}

	return opts, nil
}

func mergeOptionsIntoRequest(req *http.Request, opts *hstate.Options) {
	// 1. Replace path parameters in the URL
	req.URL.Path = replacePathParams(req.URL.Path, opts.PathParameters())

	// 2. Add query parameters to the URL
	q := req.URL.Query()
	for key, values := range opts.Query() {
		q[key] = append(q[key], values...)
	}
	req.URL.RawQuery = q.Encode()

	// 3. Add headers to the request
	for key, values := range opts.Headers() {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	// 4. Set the request body
	if len(opts.Body()) > 0 {
		req.Body = ioutil.NopCloser(bytes.NewBuffer(opts.Body()))
		req.ContentLength = int64(len(opts.Body()))
		// Set default Content-Type if not already set
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/json")
		}
	}

	if isServerRequest(req) {
		req.RequestURI = req.URL.RequestURI()
	}
}

func NewHttp(executor *Executor, resource *view.Resource) *Httper {
	return &Httper{
		executor: executor,
		resource: resource,
	}
}

func replacePathParams(path string, params map[string]string) string {
	for key, value := range params {
		placeholder := fmt.Sprintf("{%s}", key)
		path = strings.ReplaceAll(path, placeholder, url.PathEscape(value))
	}
	return path
}

func isServerRequest(req *http.Request) bool {
	// In Go's net/http, server requests have a non-nil RequestURI
	return req.RequestURI != ""
}
