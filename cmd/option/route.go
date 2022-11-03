package option

import (
	"github.com/viant/datly/router"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
)

type Route struct {
	URI            string
	ConstFileURL   string
	Cache          *view.Cache
	Method         string
	CaseFormat     string
	DateFormat     string
	CSV            *router.CSVConfig
	Declare        map[string]string
	ParameterHints sanitize.ParameterHints `json:"-"`
	Const          map[string]interface{}
}
