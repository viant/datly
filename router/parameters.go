package router

import (
	"encoding/json"
	"github.com/viant/datly/converter"
	"github.com/viant/toolbox"
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

		requestBody    interface{}
		presenceMap    map[string]interface{}
		request        *http.Request
		route          *Route
		requestBodyErr error
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
		cookies: request.Cookies(),
		request: request,
		route:   route,
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

	p.cookiesIndex = map[string]*http.Cookie{}
	for i := range p.cookies {
		p.cookiesIndex[p.cookies[i].Name] = p.cookies[i]
	}

	return "", nil
}

func (p *RequestParams) queryParam(name string, defaultValue string) string {
	values, ok := p.queryIndex[name]
	if !ok {
		return defaultValue
	}

	return values[0]
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
	if err != nil {
		return nil, err
	}

	convert, _, err := converter.Convert(string(body), unmarshaller.rType, route.CustomValidation, "", unmarshaller.unmarshal)
	if err != nil {
		return nil, err
	}

	p.presenceMap, err = unmarshaller.presence(body)

	if unmarshaller.unwrapper != nil {
		convert, err = unmarshaller.unwrapper(convert)
		if err != nil {
			return nil, err
		}
	}

	return convert, nil
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
	format := strings.ToLower(p.queryParam(FormatQuery, ""))
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
		bodyMap := map[string]interface{}{}
		return bodyMap, json.Unmarshal(b, &bodyMap)
	}
}

func (p *RequestParams) RequestBody() (interface{}, error) {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()
	if p.requestBody != nil || p.requestBodyErr != nil {
		return p.requestBody, p.requestBodyErr
	}

	body, err := p.tryParseRequestBody()
	p.requestBody, p.requestBodyErr = body, err
	return body, err
}

func (p *RequestParams) tryParseRequestBody() (interface{}, error) {
	body, err := io.ReadAll(p.request.Body)
	defer p.request.Body.Close()
	if err != nil {
		return nil, err
	}

	requestBody, err := p.parseRequestBody(body, p.route)
	if err != nil {
		return nil, err
	}

	return requestBody, nil
}
