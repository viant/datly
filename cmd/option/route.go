package option

import (
	"github.com/viant/datly/view"
)

type (
	Route struct {
		URI            string
		URIParams      map[string]bool
		Cache          *view.Cache
		Method         string
		Declare        map[string]string
		ParameterHints ParameterHints `json:"-"`
		ExecData       *ExecData      `json:"-"`
		ReadData       *ReadData      `json:"-"`
		err            error
	}

	ExecData struct {
		Meta *ViewMeta
	}

	ReadData struct {
		Table          *Table
		DataViewParams map[string]*TableParam
	}
)

func (r *Route) Err() error {
	return r.err
}

func (r *Route) SetErr(err error) {
	r.err = err
}
