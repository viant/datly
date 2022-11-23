package option

import (
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
)

type (
	RouteConfig struct {
		URI           string                 `json:",omitempty"`
		ConstFileURL  string                 `json:",omitempty"`
		Cache         *view.Cache            `json:",omitempty"`
		Method        string                 `json:",omitempty"`
		CaseFormat    string                 `json:",omitempty"`
		DateFormat    string                 `json:",omitempty"`
		CSV           *router.CSVConfig      `json:",omitempty"`
		Declare       map[string]string      `json:",omitempty"`
		Const         map[string]interface{} `json:",omitempty"`
		ResponseField string                 `json:",omitempty"`
		RequestBody   *BodyConfig            `json:",omitempty"`
		TypeSrc       *TypeSrcConfig         `json:",omitempty"`
	}

	TypeSrcConfig struct {
		URL   string
		Types []string
	}

	BodyConfig struct {
		ReturnAsResponse bool   `json:",omitempty"`
		Type             string `json:",omitempty"`
	}
)
