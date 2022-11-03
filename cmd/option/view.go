package option

import "github.com/viant/datly/view"

type ViewConfig struct {
	Connector         string
	Self              *view.SelfReference
	Cache             *view.Cache
	Warmup            map[string]interface{}
	DataViewParameter *view.Parameter `json:"-"`
	Auth              string
	Selector          *view.Config
	AllowNulls        *bool
}
