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
)

type (
	RequestParams struct {
		OutputFormat string
		InputFormat  string

		cookiesIndex map[string]*http.Cookie
		cookies      []*http.Cookie

		queryIndex url.Values
		pathIndex  map[string]string

		requestBody interface{}
		presenceMap map[string]interface{}
		request     *http.Request
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
	}

	if paramName, err := parameters.init(request, route); err != nil {
		errors := NewErrors()
		errors.AddError("", paramName, err)
		return nil, errors
	}

	return parameters, nil
}

func (p *RequestParams) init(request *http.Request, route *Route) (string, error) {
	p.pathIndex, _ = toolbox.ExtractURIParameters(route.URI, request.RequestURI)
	p.queryIndex = request.URL.Query()
	p.OutputFormat = p.outputFormat()
	p.InputFormat = p.header(HeaderContentType)

	p.cookiesIndex = map[string]*http.Cookie{}
	for i := range p.cookies {
		p.cookiesIndex[p.cookies[i].Name] = p.cookies[i]
	}

	return p.initRequestBody(request, route)
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

func (p *RequestParams) initRequestBody(request *http.Request, route *Route) (string, error) {
	if route._requestBodyType == nil {
		return "", nil
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		return "RequestBody", err
	}

	_ = request.Body.Close()
	if len(body) == 0 {
		return "", nil
	}

	return "RequestBody", p.parseRequestBody(body, route)
}

func (p *RequestParams) parseRequestBody(body []byte, route *Route) error {
	unmarshaller, err := p.unmarshaller(route)
	if err != nil {
		return err
	}

	convert, _, err := converter.Convert(string(body), unmarshaller.rType, "", unmarshaller.unmarshal)
	if err != nil {
		return err
	}

	p.presenceMap, err = unmarshaller.presence(body)

	if unmarshaller.unwrapper != nil {
		convert, err = unmarshaller.unwrapper(convert)
		if err != nil {
			return err
		}
	}

	p.requestBody = convert
	return nil
}

func (p *RequestParams) outputFormat() string {
	requestedFormat := strings.ToLower(p.queryParam(FormatQuery, ""))
	switch requestedFormat {
	case CSVQueryFormat:
		return CSVFormat
	}

	return JSONFormat
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
		unmarshal: json.Unmarshal,
		presence:  p.jsonPresenceMap(),
		rType:     route._requestBodyType,
	}, nil
}

func (p *RequestParams) jsonPresenceMap() PresenceMapFn {
	return func(b []byte) (map[string]interface{}, error) {
		bodyMap := map[string]interface{}{}
		return bodyMap, json.Unmarshal(b, &bodyMap)
	}
}
