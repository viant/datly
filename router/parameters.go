package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox"
	"github.com/viant/xunsafe"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
)

type (
	RequestParams struct {
		sync.Mutex
		OutputFormat string
		InputFormat  string

		cookiesIndex map[string]*http.Cookie
		cookies      []*http.Cookie

		queryIndex url.Values
		pathIndex  map[string]string

		request *http.Request
		route   *Route

		requestBodyContent []byte
		bodyParam          interface{}
		bodyPathParam      map[string]interface{}
		requestBodyErr     error
		readRequestBody    bool
		accessors          *types.Accessors
	}

	PresenceMapFn func([]byte) (map[string]interface{}, error)
	Unwrapper     func(interface{}) (interface{}, error)
	Marshaller    struct {
		unmarshal converter.Unmarshaller
		presence  PresenceMapFn
		unwrapper Unwrapper
		rType     reflect.Type
	}
)

func NewRequestParameters(request *http.Request, route *Route) (*RequestParams, error) {
	parameters := &RequestParams{
		cookies:       request.Cookies(),
		request:       request,
		route:         route,
		accessors:     route._accessors,
		bodyPathParam: map[string]interface{}{},
		cookiesIndex:  map[string]*http.Cookie{},
	}

	if paramName, err := parameters.init(request, route); err != nil {
		errors := NewErrors()
		errors.AddError("", paramName, err)
		return nil, errors
	}

	return parameters, nil
}

func (p *RequestParams) init(request *http.Request, route *Route) (string, error) {
	p.pathIndex, _ = toolbox.ExtractURIParameters(route.URI, request.URL.Path)
	p.queryIndex = request.URL.Query()
	p.OutputFormat = p.outputFormat(route)
	p.InputFormat = p.header(HeaderContentType)

	for i := range p.cookies {
		p.cookiesIndex[p.cookies[i].Name] = p.cookies[i]
	}

	return "", nil
}

func (p *RequestParams) queryParam(name string) (string, bool) {
	values, ok := p.queryIndex[name]
	if !ok {
		return "", ok
	}

	return values[0], true
}

func (p *RequestParams) pathVariable(name string, defaultValue string) string {
	value, ok := p.pathIndex[name]
	if !ok {
		return defaultValue
	}

	return value
}

func (p *RequestParams) header(name string) string {
	result := p.request.Header.Get(name)
	if result == "" {
		result = p.request.Header.Get(strings.ToLower(name))
	}
	return result
}

func (p *RequestParams) cookie(name string) string {
	cookie, ok := p.cookiesIndex[name]
	if !ok {
		return ""
	}

	return cookie.Value
}

func (p *RequestParams) parseRequestBody(body []byte, route *Route) (interface{}, error) {
	unmarshaller, err := p.unmarshaller(route)
	if err != nil || unmarshaller.rType == nil {
		return nil, err
	}

	converted, _, err := converter.Convert(string(body), unmarshaller.rType, route.CustomValidation, "", unmarshaller.unmarshal)
	if err != nil {
		return nil, err
	}

	if unmarshaller.unwrapper != nil {
		converted, err = unmarshaller.unwrapper(converted)
		if err != nil {
			return nil, err
		}
	}

	return converted, nil
}

func (p *RequestParams) outputFormat(route *Route) string {

	format := p.outputQueryFormat(route)

	switch format {
	case CSVQueryFormat:
		return CSVFormat
	case TabularJSONQueryFormat:
		return TabularJSONFormat
	}

	return JSONFormat
}

func (p *RequestParams) outputQueryFormat(route *Route) string {
	param, _ := p.queryParam(FormatQuery)
	format := strings.ToLower(param)
	if format == "" {
		format = route.Output.DataFormat
	}
	return format
}

func (p *RequestParams) unmarshaller(route *Route) (*Marshaller, error) {
	switch p.InputFormat {
	case CSVFormat:
		if route.CSV == nil {
			return nil, UnsupportedFormatErr(CSVFormat)
		}

		return &Marshaller{
			unmarshal: route.CSV.Unmarshal,
			presence:  route.CSV.presenceMap(),
			unwrapper: route.CSV.unwrapIfNeeded,
			rType:     route._requestBodySlice.Type,
		}, nil
	}

	return &Marshaller{
		unmarshal: func(bytes []byte, i interface{}) error {
			return route._marshaller.Unmarshal(bytes, i, route.unmarshallerInterceptors(p), p.request)
		},
		presence: p.jsonPresenceMap(),
		rType:    route._requestBodyType,
	}, nil
}

func (p *RequestParams) jsonPresenceMap() PresenceMapFn {
	return func(b []byte) (map[string]interface{}, error) {
		b = bytes.TrimSpace(b)
		bodyMap := map[string]interface{}{}
		if len(b) > 0 && b[0] == '[' || len(b) == 0 {
			return bodyMap, nil
		}

		return bodyMap, json.Unmarshal(b, &bodyMap)
	}
}

func (p *RequestParams) paramRequestBody(ctx context.Context, param *view.Parameter, options ...interface{}) (interface{}, error) {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()
	err := p.readBody()
	if err != nil {
		return nil, err
	}

	if param == nil {
		return nil, err
	}

	if param == nil || param.In.Name == "" {
		return p.bodyParam, nil
	}

	value, err := p.extractBodyByPath(param, err)
	if value == nil || err != nil {
		return nil, err
	}

	return value, err
}

func (p *RequestParams) extractBodyByPath(param *view.Parameter, err error) (interface{}, error) {
	if param.In.Name == "" {
		return p.bodyParam, nil
	}

	value, ok := p.bodyPathParam[param.In.Name]
	if ok {
		return value, nil
	}

	aQuery, ok := p.route.bodyParamQuery[param.In.Name]
	if !ok {
		return nil, fmt.Errorf("unable to locate param aQuery: %s", param.Name)
	}

	if value, err = aQuery.First(p.bodyParam); err == nil {
		ptr := xunsafe.AsPointer(value)
		value = aQuery.field.Value(ptr)
		p.bodyPathParam[param.In.Name] = value
	}

	return value, err
}

func (p *RequestParams) readBody() error {
	if p.request.Body == nil || p.readRequestBody {
		return p.requestBodyErr
	}
	body, err := io.ReadAll(p.request.Body)
	defer func() {
		p.request.Body.Close()
		p.readRequestBody = true
	}()
	if err != nil {
		p.requestBodyErr = err
		return err
	}
	p.requestBodyContent = body
	requestData, err := p.parseRequestBody(body, p.route)
	if err != nil {
		p.requestBodyErr = err
	}
	p.bodyParam = requestData
	return p.requestBodyErr
}

func (p *RequestParams) ExtractHttpParam(ctx context.Context, param *view.Parameter, options ...interface{}) (interface{}, error) {
	value, err := p.extractHttpParam(ctx, param, options)
	if err != nil || value == nil {
		return nil, err
	}

	return transformIfNeeded(ctx, param, value, options...)
}

func (p *RequestParams) extractHttpParam(ctx context.Context, param *view.Parameter, options []interface{}) (interface{}, error) {
	switch param.In.Kind {
	case view.KindPath:
		return p.convert(true, p.pathVariable(param.In.Name, ""), param, options...)
	case view.KindQuery:
		pValue, ok := p.queryParam(param.In.Name)
		return p.convert(ok, pValue, param, options...)
	case view.KindRequestBody:
		body, err := p.paramRequestBody(ctx, param, options...)
		if err != nil {
			return nil, err
		}

		return body, nil
	case view.KindHeader:
		return p.convert(true, p.header(param.In.Name), param, options...)
	case view.KindCookie:
		return p.convert(true, p.cookie(param.In.Name), param, options...)
	}

	return nil, fmt.Errorf("unsupported param kind %v", param.In.Kind)
}

func (p *RequestParams) Header() http.Header {
	return p.request.Header
}

func (p *RequestParams) BodyContent() ([]byte, error) {
	return p.requestBodyContent, p.readBody()
}

func (p *RequestParams) RequestBody() (interface{}, error) {
	return p.bodyParam, p.readBody()
}
