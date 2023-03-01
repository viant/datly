package cmd

import (
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/template/sanitize"
)

type Parameter struct {
	Repeated bool             `json:",omitempty" yaml:",omitempty"`
	FullName string           `json:"-" yaml:"-"`
	Assumed  bool             `json:",omitempty" yaml:",omitempty"`
	Typer    []sanitize.Typer `json:",omitempty" yaml:",omitempty"`
	SQL      string           `json:",omitempty" yaml:",omitempty"`
	Multi    bool             `json:",omitempty" yaml:",omitempty"`
	Has      bool             `json:",omitempty" yaml:",omitempty"`
	SQLCodec bool             `json:",omitempty" yaml:",omitempty"`
	option.ParameterConfig
}
