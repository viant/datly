package cmd

import (
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
)

type Parameter struct {
	Auth           string
	Id             string           `json:",omitempty" yaml:",omitempty"`
	Name           string           `json:",omitempty" yaml:",omitempty"`
	Kind           string           `json:",omitempty" yaml:",omitempty"`
	Required       *bool            `json:",omitempty" yaml:",omitempty"`
	DataType       string           `json:",omitempty" yaml:",omitempty"`
	Repeated       bool             `json:",omitempty" yaml:",omitempty"`
	ExpectReturned *int             `json:",omitempty" yaml:",omitempty"`
	Codec          string           `json:",omitempty" yaml:",omitempty"`
	FullName       string           `json:"-" yaml:"-"`
	Assumed        bool             `json:",omitempty" yaml:",omitempty"`
	Typer          sanitize.Typer   `json:",omitempty" yaml:",omitempty"`
	SQL            string           `json:",omitempty" yaml:",omitempty"`
	Cardinality    view.Cardinality `json:",omitempty" yaml:",omitempty"`
	Multi          bool             `json:",omitempty" yaml:",omitempty"`
	Const          interface{}      `json:",omitempty" yaml:",omitempty"`
}
