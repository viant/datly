package router

import (
	"github.com/viant/datly/converter"
	"github.com/viant/toolbox"
	"io"
	"net/http"
	"net/url"
)

type RequestParams struct {
	cookiesIndex map[string]*http.Cookie
	cookies      []*http.Cookie

	queryIndex url.Values
	pathIndex  map[string]string

	requestBody interface{}
	request     *http.Request
}

func NewRequestParameters(request *http.Request, route *Route) (*RequestParams, error) {
	parameters := &RequestParams{
		cookies: request.Cookies(),
		request: request,
	}

	if err := parameters.init(request, route); err != nil {
		errors := NewErrors()
		errors.AddError("", "RequestBody", err)
		return nil, errors
	}

	return parameters, nil
}

func (p *RequestParams) init(request *http.Request, route *Route) error {
	p.pathIndex, _ = toolbox.ExtractURIParameters(route.URI, request.RequestURI)
	p.queryIndex = request.URL.Query()

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
	return p.request.Header.Get(name)
}

func (p *RequestParams) cookie(name string) string {
	cookie, ok := p.cookiesIndex[name]
	if !ok {
		return ""
	}

	return cookie.Value
}

func (p *RequestParams) initRequestBody(request *http.Request, route *Route) error {
	if route._requestBodyType == nil {
		return nil
	}

	defer request.Body.Close()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return err
	}

	if len(body) == 0 {
		return nil
	}

	convert, _, err := converter.Convert(string(body), route._requestBodyType, "")
	if err != nil {
		return err
	}

	p.requestBody = convert
	return nil
}
