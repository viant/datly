package data

import (
	"fmt"
	"github.com/viant/datly/v1/shared"
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

	errors        *shared.Errors
	pathVariables map[string]string
}

func (s *Session) DataType() reflect.Type {
	return s.View.DataType()
}

func (s *Session) NewReplacement(view *View) rdata.Map {
	aMap := rdata.NewMap()
	aMap.SetValue(string(shared.DataViewName), view.Name)
	aMap.SetValue(string(shared.SubjectName), s.Subject)

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
		var ok bool
		s.pathVariables, ok = toolbox.ExtractURIParameters(s.MatchedPath, s.HttpRequest.URL.Path)
		if !ok {
			return fmt.Errorf("route path doesn't match %v request URI %v", s.MatchedPath, s.HttpRequest.URL.Path)
		}

	}

	return nil
}

func (s *Session) CollectError(err error) {
	if s.errors == nil {
		s.errors = shared.NewErrors(0)
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
	cookie, err := s.HttpRequest.Cookie(name)
	if err != nil {
		return ""
	}
	return cookie.Value
}

func (s *Session) PathVariable(name string) string {
	return s.pathVariables[name]
}
