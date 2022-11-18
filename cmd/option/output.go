package option

import (
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
)

type OutputConfig struct {
	Style         string
	ResponseField string
	Kind          string
	Cardinality   view.Cardinality
}

func (o *OutputConfig) IsMany() bool {
	return o.Cardinality == view.Many
}

func (o *OutputConfig) IsBasic() bool {
	return o.Style != string(router.ComprehensiveStyle) && o.ResponseField == ""
}

func (o *OutputConfig) Field() string {
	if o.IsBasic() {
		return ""
	}

	if o.ResponseField == "" {
		return "Data"
	}

	return o.ResponseField
}
