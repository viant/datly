package translator

import (
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
)

type (
	Rule struct {
		Route
		Namespaces
		Root string
	}

	Route struct {
		router.Route

		Async        *AsyncConfig              `json:",omitempty"`
		ConstFileURL string                    `json:",omitempty"`
		Cache        *view.Cache               `json:",omitempty"`
		CSV          *router.CSVConfig         `json:",omitempty"`
		Const        map[string]interface{}    `json:",omitempty"`
		ConstURL     string                    `json:",omitempty"`
		Field        string                    `json:",omitempty"`
		RequestBody  *BodyConfig               `json:",omitempty"`
		TypeSrc      *TypeSrcConfig            `json:",omitempty"`
		ResponseBody *ResponseBodyConfig       `json:",omitempty"`
		Package      string                    `json:",omitempty"`
		Router       *RouterConfig             `json:",omitempty" yaml:",omitempty"`
		DataFormat   string                    `json:",omitempty"`
		TabularJSON  *router.TabularJSONConfig `json:",omitempty"`
		HandlerType  string                    `json:",omitempty"`
		StateType    string                    `json:",omitempty"`
	}

	TypeSrcConfig struct {
		URL            string
		Types          []string
		Alias          string
		ForceGoTypeUse bool
	}

	RouterConfig struct {
		RouterURL string `json:",omitempty" yaml:",omitempty"`
		URL       string `json:",omitempty" yaml:",omitempty"`
		Routes    []struct {
			SourceURL string
		}
	}

	BodyConfig struct {
		DataType string `json:",omitempty"`
	}

	ResponseBodyConfig struct {
		From string
	}

	AsyncConfig struct {
		PrincipalSubject string `json:",omitempty" yaml:",omitempty"`
		Connector        string `json:",omitempty" yaml:",omitempty"`
		EnsureTable      *bool  `json:",omitempty" yaml:",omitempty"`
		ExpiryTimeInS    int    `json:",omitempty" yaml:",omitempty"`
		MarshalRelations *bool  `json:",omitempty" yaml:",omitempty"`
		Dataset          string `json:",omitempty" yaml:",omitempty"`
		BucketURL        string `json:",omitempty" yaml:",omitempty"`
	}
)

func NewRule() *Rule {
	return &Rule{}
}
