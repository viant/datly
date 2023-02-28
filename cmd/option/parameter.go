package option

import "github.com/viant/datly/view"

type ParameterConfig struct {
	ParamMeta
	Auth           string           `json:",omitempty" yaml:",omitempty"`
	Connector      string           `json:",omitempty" yaml:",omitempty"`
	Id             string           `json:",omitempty" yaml:",omitempty"`
	Name           string           `json:",omitempty" yaml:",omitempty"`
	Kind           string           `json:",omitempty" yaml:",omitempty"`
	Required       *bool            `json:",omitempty" yaml:",omitempty"`
	DataType       string           `json:",omitempty" yaml:",omitempty"`
	ExpectReturned *int             `json:",omitempty" yaml:",omitempty"`
	Codec          string           `json:",omitempty" yaml:",omitempty"`
	Target         *string          `json:",omitempty" yaml:",omitempty"`
	Const          interface{}      `json:",omitempty" yaml:",omitempty"`
	CodecType      string           `json:",omitempty" yaml:",omitempty"`
	Cardinality    view.Cardinality `json:",omitempty" yaml:",omitempty"`
	StatusCode     *int             `json:",omitempty" yaml:",omitempty"`
}

type ParamMeta struct {
	Util bool `json:",omitempty" yaml:",omitempty"`
}
