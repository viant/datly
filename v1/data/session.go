package data

import (
	"github.com/viant/datly/v1/shared"
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

	errors *shared.Errors
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

func (s *Session) Init() {
	s.Selectors.Init()
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
