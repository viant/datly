package httputils

import (
	"github.com/viant/toolbox"
	"net/http"
	"net/url"
	"strings"
)

type Request struct {
	Url           string                  `velty:"-"`
	QueryParams   url.Values              `velty:"-"`
	PathVariables map[string]string       `velty:"-"`
	Headers       http.Header             `velty:"-"`
	cookies       map[string]*http.Cookie `velty:"-"`
	request       *http.Request           `velty:"-"`
}

func NewRequest(cookies map[string]*http.Cookie, pathVariables map[string]string, queryParams url.Values, headers http.Header) *Request {
	return &Request{
		QueryParams:   queryParams,
		PathVariables: pathVariables,
		Headers:       headers,
		cookies:       cookies,
	}
}

func RequestOf(r *http.Request, templateURI string) (*Request, error) {
	cookies := r.Cookies()
	cookiesMap := map[string]*http.Cookie{}
	for _, cookie := range cookies {
		cookiesMap[cookie.Name] = cookie
	}

	pathVariables := map[string]string{}
	if templateURI != "" {
		uriParams, ok := toolbox.ExtractURIParameters(templateURI, r.RequestURI)
		if ok {
			pathVariables = uriParams
		}
	}

	return &Request{
		Url:           templateURI,
		QueryParams:   r.URL.Query(),
		PathVariables: pathVariables,
		Headers:       r.Header,
		cookies:       cookiesMap,
		request:       r,
	}, nil
}

func (r *Request) QueryParam(name string) string {
	return r.QueryParams.Get(name)
}

func (r *Request) HasQuery(name string) bool {
	return r.QueryParams.Has(name)
}

func (r *Request) PathVariable(name string) string {
	return r.PathVariables[name]
}

func (r *Request) HasPathVariable(name string) bool {
	_, ok := r.PathVariables[name]
	return ok
}
func (r *Request) Cookie(name string) *http.Cookie {
	return r.cookies[name]
}

func (r *Request) HasCookie(name string) bool {
	_, ok := r.cookies[name]
	return ok
}

func (r *Request) Path() string {
	return r.request.URL.Path
}

func (r *Request) RelativePath() string {
	if r.Url == "" {
		return r.request.URL.Path
	}

	return strings.Replace(r.request.URL.Path, r.Url, "", 1)
}
