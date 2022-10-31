package option

import (
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
)

type (
	ViewMeta struct {
		Mode           string            `json:",omitempty" yaml:",omitempty"`
		Parameters     []*Parameter      `json:",omitempty" yaml:",omitempty"`
		Source         string            `json:",omitempty" yaml:",omitempty"`
		From           string            `json:",omitempty" yaml:",omitempty"`
		Expressions    []string          `json:",omitempty" yaml:",omitempty"`
		ParameterTypes map[string]string `json:",omitempty" yaml:",omitempty"`
		Updates        []string          `json:",omitempty" yaml:",omitempty"`
		Inserts        []string          `json:",omitempty" yaml:",omitempty"`
		index          map[string]int
		variables      map[string]bool
	}

	Parameter struct {
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
	}
)

func BoolPtr(b bool) *bool {
	return &b
}
