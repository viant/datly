package option

import (
	"github.com/viant/datly/config"
	"github.com/viant/datly/view"
)

type (
	ParameterConfig struct {
		ParamMeta
		CodecConfig
		Auth              string      `json:",omitempty" yaml:",omitempty"`
		Connector         string      `json:",omitempty" yaml:",omitempty"`
		Id                string      `json:",omitempty" yaml:",omitempty"`
		Name              string      `json:",omitempty" yaml:",omitempty"`
		Kind              string      `json:",omitempty" yaml:",omitempty"`
		Required          *bool       `json:",omitempty" yaml:",omitempty"`
		DataType          string      `json:",omitempty" yaml:",omitempty"`
		MinAllowedRecords *int        `json:",omitempty" yaml:",omitempty"`
		MaxAllowedRecords *int        `json:",omitempty" yaml:",omitempty"`
		ExpectReturned    *int        `json:",omitempty" yaml:",omitempty"`
		Location          *string     `json:",omitempty" yaml:",omitempty"`
		Const             interface{} `json:",omitempty" yaml:",omitempty"`

		Cardinality view.Cardinality `json:",omitempty" yaml:",omitempty"`
		StatusCode  *int             `json:",omitempty" yaml:",omitempty"`
		Qualifiers  []*Qualifier     `json:",omitempty"`
		Predicate   []*config.PredicateConfig
		Tag         string `json:",omitempty" yaml:",omitempty"`
	}

	Qualifier struct {
		Column string
		Value  string
	}

	CodecConfig struct {
		CodecType    string `json:",omitempty" yaml:",omitempty"`
		Codec        string `json:",omitempty" yaml:",omitempty"`
		CodecHandler string `json:",omitempty" yaml:",omitempty"`
	}
)

type ParamMeta struct {
	Util bool `json:",omitempty" yaml:",omitempty"`
}
