package option

import "github.com/viant/datly/view"

type ParameterConfig struct {
	Auth           string
	Id             string           `json:",omitempty" yaml:",omitempty"`
	Name           string           `json:",omitempty" yaml:",omitempty"`
	Kind           string           `json:",omitempty" yaml:",omitempty"`
	Required       *bool            `json:",omitempty" yaml:",omitempty"`
	DataType       string           `json:",omitempty" yaml:",omitempty"`
	ExpectReturned *int             `json:",omitempty" yaml:",omitempty"`
	Codec          string           `json:",omitempty" yaml:",omitempty"`
	Target         string           `json:",omitempty" yaml:",omitempty"`
	Const          interface{}      `json:",omitempty" yaml:",omitempty"`
	Cardinality    view.Cardinality `json:",omitempty" yaml:",omitempty"`
}
