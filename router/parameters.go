package router

import (
	"github.com/viant/toolbox"
	"net/http"
	"net/url"
)

type RequestParams struct {
	cookiesIndex map[string]*http.Cookie
	cookies      []*http.Cookie

	queryIndex url.Values
	pathIndex  map[string]string

	request *http.Request
}

func NewRequestParameters(request *http.Request, url string) *RequestParams {
	parameters := &RequestParams{
		cookies: request.Cookies(),
		request: request,
	}
	parameters.init(request, url)
	return parameters
}

func (p *RequestParams) init(request *http.Request, url string) {
	p.pathIndex, _ = toolbox.ExtractURIParameters(url, request.RequestURI)
	p.queryIndex = request.URL.Query()

	p.cookiesIndex = map[string]*http.Cookie{}
	for i := range p.cookies {
		p.cookiesIndex[p.cookies[i].Name] = p.cookies[i]
	}

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
