package option

import "github.com/viant/datly/view"

const (
	ExecKindService = "service"
	ExecKindDML     = "dml"
)

type (
	ViewConfig struct {
		ViewPregenConfig
		Connector         string                 `json:",omitempty"`
		Self              *view.SelfReference    `json:",omitempty"`
		Cache             *view.Cache            `json:",omitempty"`
		Warmup            map[string]interface{} `json:",omitempty"`
		DataViewParameter *view.Parameter        `json:"-"`
		Auth              string                 `json:",omitempty"`
		Selector          *view.Config           `json:",omitempty"`
		AllowNulls        *bool                  `json:",omitempty"`
	}

	ViewPregenConfig struct {
		ExecKind     string `json:",omitempty"`
		FetchRecords bool   `json:",omitempty"`
	}
)
