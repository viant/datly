package router

import (
	"bytes"
	"context"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"io"
	"net/http"
	"net/url"
	"reflect"
)

type (
	Httper struct {
		executor *HandlerExecutor
		resource *view.Resource
	}

	paramKey struct {
		kind view.Kind
		in   string
	}
)

func (h *Httper) RawRequest() *http.Request {
	return h.executor.request
}

func (h *Httper) RequestOf(ctx context.Context, state interface{}) (*http.Request, error) {
	params, err := h.executor.RequestParams(ctx)
	if err != nil {
		return nil, err
	}

	stater := h.executor.route.NewStater(h.executor.request, params)
	of := reflect.TypeOf(state)
	updater, err := stater.getUpdater(ctx, of)
	if err != nil {
		return nil, err
	}

	return h.buildRequest(ctx, updater.params)
}

func (h *Httper) RouteRequest(ctx context.Context) (*http.Request, error) {
	var params []*view.Parameter
	err := h.appendParams(h.executor.route.View, &params)
	if err != nil {
		return nil, err
	}

	return h.buildRequest(ctx, params)
}

func (h *Httper) FailWithCode(statusCode int, err error) error {
	return httputils.NewHttpMessageError(statusCode, err)
}

func (h *Httper) buildRequest(ctx context.Context, params []*view.Parameter) (*http.Request, error) {
	newRequest := *h.executor.request

	queryParams := url.Values{}
	headers := url.Values{}
	body := bytes.NewReader(nil)

	indexed := map[paramKey]bool{}

	requestParams, err := h.executor.RequestParams(ctx)
	if err != nil {
		return nil, err
	}

	for _, param := range params {
		src := param.In.Name
		aKey := paramKey{
			kind: param.In.Kind,
			in:   src,
		}

		if indexed[aKey] {
			continue
		}
		indexed[aKey] = true

		switch param.In.Kind {
		case view.KindQuery:
			queryParam, ok := requestParams.queryParam(src)
			if ok {
				queryParams.Add(src, queryParam)
			}

		case view.KindRequestBody:
			content, err := requestParams.BodyContent()
			if err != nil {
				return nil, err
			}

			body = bytes.NewReader(content)

		case view.KindHeader:
			headers.Add(src, requestParams.header(src))
		}
	}

	newRequest.Body = io.NopCloser(body)
	newRequest.URL = h.replaceQuery(h.executor.request, queryParams)
	return &newRequest, nil
}

func (h *Httper) replaceQuery(request *http.Request, params url.Values) *url.URL {
	URL := *request.URL
	URL.RawQuery = params.Encode()
	return &URL
}

func (h *Httper) appendParams(aView *view.View, dst *[]*view.Parameter) error {
	for _, parameter := range aView.Template.Parameters {
		*dst = append(*dst, parameter)
		if parameter.In.Kind == view.KindDataView {
			paramView, err := h.resource.View(parameter.In.Name)
			if err != nil {
				return err
			}

			if err = h.appendParams(paramView, dst); err != nil {
				return err
			}
		}
	}

	for _, relation := range aView.With {
		if err := h.appendParams(&relation.Of.View, dst); err != nil {
			return err
		}
	}

	return nil
}
