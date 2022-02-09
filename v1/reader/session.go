package reader

import (
	"github.com/viant/datly/v1/data"
	"reflect"
)

type Session struct {
	Dest     interface{} //  slice
	View     *data.View
	Selector *data.ClientSelector
}

func (s *Session) SelectorInUse() data.Selector {
	if s.Selector != nil {
		return s.Selector
	} else {
		return s.View.Default
	}
}

func (s *Session) DataType() reflect.Type {
	if s.Selector != nil {
		return s.Selector.GetType()
	} else {
		return s.View.DataType()
	}
}
