package option

import (
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
)

type OutputConfig struct {
	Style       string
	Field       string
	Kind        string
	Cardinality view.Cardinality
}

func (o *OutputConfig) IsMany() bool {
	return o.Cardinality == "" || o.Cardinality == view.Many
}

func (o *OutputConfig) IsBasic() bool {
	return o.Style != string(router.ComprehensiveStyle) && o.Field == ""
}

func (o *OutputConfig) GetField() string {
	if o.IsBasic() {
		return ""
	}

	if o.Field == "" {
		return "Data"
	}

	return o.Field
}
