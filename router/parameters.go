package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
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
		OutputContentType string
		InputDataFormat   string

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
	}
)

func NewRequestParameters(request *http.Request, route *Route) (*RequestParams, error) {
	parameters := &RequestParams{
		cookies:       request.Cookies(),
		request:       request,
		route:         route,
		bodyPathParam: map[string]interface{}{},
		cookiesIndex:  map[string]*http.Cookie{},
	}

	if paramName, err := parameters.init(request, route); err != nil {
		errors := httputils.NewErrors()
		errors.AddError("", paramName, err)
		return nil, errors
	}

	return parameters, nil
}

func (p *RequestParams) init(request *http.Request, route *Route) (string, error) {
	p.pathIndex, _ = toolbox.ExtractURIParameters(route.URI, request.URL.Path)
	p.queryIndex = request.URL.Query()
	p.OutputContentType = p.outputContentType(route)
	p.InputDataFormat = p.header(HeaderContentType)
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
	unmarshaller := route.Marshaller(p.request)
	if unmarshaller.Type == nil {
		return nil, nil
	}
	converted, _, err := converter.Convert(string(body), unmarshaller.Type, route.CustomValidation, "", unmarshaller.Unmarshal)
	return converted, err
}

func (p *RequestParams) outputContentType(route *Route) string {

	format := p.dataFormat(route)
	switch format {
	case XLSFormat, XLSContentType:
		return XLSContentType
	case CSVFormat, CSVContentType:
		return CSVContentType
	case XMLFormat, XMLContentType:
		return XMLContentType
	case JSONDataFormatTabular:
		return TabularJSONFormat
	}
	return JSONContentType
}

func (p *RequestParams) dataFormat(route *Route) string {
	param, _ := p.queryParam(FormatQuery)
	format := strings.ToLower(param)
	if format == "" {
		format = route.Output.DataFormat
	}
	if format == "" {
		format = JSONFormat
	}
	return format
}

func (p *RequestParams) paramRequestBody(ctx context.Context, param *state.Parameter, options ...interface{}) (interface{}, error) {
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

func (p *RequestParams) extractBodyByPath(param *state.Parameter, err error) (interface{}, error) {
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

func (p *RequestParams) ExtractHttpParam(ctx context.Context, param *state.Parameter, options ...interface{}) (interface{}, error) {
	value, err := p.extractHttpParam(ctx, param, options)
	if err != nil || value == nil {
		return nil, err
	}
	if param.Output == nil {
		return value, nil
	}
	return param.Output.Transform(ctx, value, state.AsCodecOptions(options)...)
}

func (p *RequestParams) extractHttpParam(ctx context.Context, param *state.Parameter, options []interface{}) (interface{}, error) {
	switch param.In.Kind {
	case state.KindPath:
		return p.convert(true, p.pathVariable(param.In.Name, ""), param)
	case state.KindQuery:
		pValue, ok := p.queryParam(param.In.Name)
		return p.convert(ok, pValue, param)
	case state.KindRequestBody:
		body, err := p.paramRequestBody(ctx, param, options...)
		if err != nil {
			return nil, err
		}

		return body, nil
	case state.KindHeader:
		return p.convert(true, p.header(param.In.Name), param)
	case state.KindCookie:
		return p.convert(true, p.cookie(param.In.Name), param)
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

func BuildParameter(field reflect.StructField) (*state.Parameter, error) {
	result := &state.Parameter{}
	paramTag := view.ParseTag("datly")
	result.Name = field.Name
	result.In = &state.Location{Kind: state.Kind(paramTag.Kind), Name: paramTag.In}
	result.Schema = state.NewSchema(field.Type)
	return result, nil
}
