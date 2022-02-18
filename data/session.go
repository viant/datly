package data

import (
	"fmt"
	shared2 "github.com/viant/datly/shared"
	"github.com/viant/datly/sql"
	"github.com/viant/toolbox"
	rdata "github.com/viant/toolbox/data"
	"net/http"
	"reflect"
)

type Session struct {
	Dest          interface{} //slice
	View          *View
	Selectors     Selectors
	AllowUnmapped bool
	Subject       string
	HttpRequest   *http.Request
	MatchedPath   string

	errors *shared2.Errors

	pathVariables map[string]string
	cookies       map[string]string
	headers       map[string]string
	queryParams   map[string]string
}

func (s *Session) DataType() reflect.Type {
	return s.View.DataType()
}

func (s *Session) NewReplacement(view *View) rdata.Map {
	aMap := rdata.NewMap()
	aMap.SetValue(string(shared2.DataViewName), view.Name)
	aMap.SetValue(string(shared2.SubjectName), s.Subject)

	return aMap
}

func (s *Session) Init() error {
	s.Selectors.Init()

	if _, ok := s.Dest.(*interface{}); !ok {
		viewType := reflect.PtrTo(s.View.Schema.SliceType())
		destType := reflect.TypeOf(s.Dest)
		if viewType != destType {
			return fmt.Errorf("type mismatch, view slice type is: %v while destination type is %v", viewType.String(), destType.String())
		}
	}

	if s.HttpRequest != nil {
		uriParams, ok := toolbox.ExtractURIParameters(s.MatchedPath, s.HttpRequest.URL.Path)
		if !ok {
			return fmt.Errorf("route path doesn't match %v request URI %v", s.MatchedPath, s.HttpRequest.URL.Path)
		}

		if err := s.indexUriParams(uriParams); err != nil {
			return err
		}

		if err := s.indexCookies(); err != nil {
			return err
		}

		if err := s.indexHeaders(); err != nil {
			return err
		}

		if err := s.indexQueryParams(); err != nil {
			return err
		}
	}

	for _, selector := range s.Selectors {
		if selector.Criteria != nil {
			if _, err := sql.Parse([]byte(selector.Criteria.Expression)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Session) CollectError(err error) {
	if s.errors == nil {
		s.errors = shared2.NewErrors(0)
	}

	s.errors.Append(err)
}

func (s *Session) Error() error {
	if s.errors == nil {
		return nil
	}
	return s.errors.Error()
}

func (s *Session) Header(name string) string {
	headerValues := s.HttpRequest.Header[name]
	headerValue := ""
	if len(headerValues) > 0 {
		headerValue = headerValues[0]
	}

	return headerValue
}

func (s *Session) Cookie(name string) string {
	return s.cookies[name]
}

func (s *Session) PathVariable(name string) string {
	return s.pathVariables[name]
}

func (s *Session) shouldIndexCookie(cookie *http.Cookie) bool {
	return s.View.shouldIndexCookie(cookie)
}

func (s *Session) indexCookies() error {
	s.cookies = make(map[string]string)
	cookies := s.HttpRequest.Cookies()
	for i := range cookies {
		if s.shouldIndexCookie(cookies[i]) {
			_, err := sql.Parse([]byte(cookies[i].Value))
			if err != nil {
				return err
			}
			s.cookies[cookies[i].Name] = cookies[i].Value
		}
	}
	return nil
}

func (s *Session) indexUriParams(params map[string]string) error {
	s.pathVariables = make(map[string]string)
	for key, val := range params {
		if s.View.shouldIndexUriParam(key) {
			_, err := sql.Parse([]byte(val))
			if err != nil {
				return err
			}
			s.pathVariables[key] = val
		}
	}
	return nil
}

func (s *Session) indexHeaders() error {
	s.headers = make(map[string]string)
	for key, val := range s.HttpRequest.Header {
		if s.View.shouldIndexHeader(key) {
			_, err := sql.Parse([]byte(val[0]))
			if err != nil {
				return err
			}
			s.headers[key] = val[0]
		}
	}

	return nil
}

func (s *Session) indexQueryParams() error {
	values := s.HttpRequest.URL.Query()
	s.queryParams = make(map[string]string)
	for k, val := range values {
		if s.View.shouldIndexQueryParam(k) {
			_, err := sql.Parse([]byte(val[0]))
			if err != nil {
				return err
			}
			s.queryParams[k] = val[0]
		}
	}
	return nil
}
