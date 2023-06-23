package option

import (
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"strings"
)

type (
	RouteConfig struct {
		Async            *AsyncConfig              `json:",omitempty"`
		URI              string                    `json:",omitempty"`
		ConstFileURL     string                    `json:",omitempty"`
		Cache            *view.Cache               `json:",omitempty"`
		CustomValidation bool                      `json:",omitempty"`
		Method           string                    `json:",omitempty"`
		CaseFormat       string                    `json:",omitempty"`
		DateFormat       string                    `json:",omitempty"`
		CSV              *router.CSVConfig         `json:",omitempty"`
		Declare          map[string]string         `json:",omitempty"`
		Const            map[string]interface{}    `json:",omitempty"`
		ConstURL         string                    `json:",omitempty"`
		Field            string                    `json:",omitempty"`
		RequestBody      *BodyConfig               `json:",omitempty"`
		TypeSrc          *TypeSrcConfig            `json:",omitempty"`
		ResponseBody     *ResponseBodyConfig       `json:",omitempty"`
		Package          string                    `json:",omitempty"`
		Router           *RouterConfig             `json:",omitempty" yaml:",omitempty"`
		DataFormat       string                    `json:",omitempty"`
		TabularJSON      *router.TabularJSONConfig `json:",omitempty"`
		HandlerType      string                    `json:",omitempty"`
		StateType        string                    `json:",omitempty"`
	}

	TypeSrcConfig struct {
		URL   string
		Types []string
		Alias string
	}

	BodyConfig struct {
		DataType string `json:",omitempty"`
	}

	ResponseBodyConfig struct {
		From string
	}
)

func (r *RouteConfig) StatePackage() string {
	if r.StateType == "" {
		return ""
	}
	index := strings.LastIndex(r.StateType, ".")
	if index == -1 {
		return ""
	}
	return r.StateType[:index]
}
