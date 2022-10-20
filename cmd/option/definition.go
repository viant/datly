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

func NewViewMeta() *ViewMeta {
	return &ViewMeta{index: map[string]int{}}
}

func (m *ViewMeta) AddParameter(param *Parameter) {
	if m.variables != nil && m.variables[param.Name] || !sanitize.CanBeParam(param.Name) {
		return
	}

	if param.Multi {
		param.Cardinality = view.Many
	}

	if index, ok := m.index[param.Id]; ok {
		parameter := m.Parameters[index]
		parameter.Multi = param.Multi || parameter.Multi
		if parameter.Multi {
			parameter.Cardinality = view.Many
		}

		parameter.Repeated = parameter.Repeated || param.Repeated

		parameter.Required = BoolPtr((parameter.Required != nil && *parameter.Required) || (param.Required != nil && *param.Required))
		if parameter.Assumed {
			parameter.DataType = param.DataType
		}

		return
	}

	m.index[param.Id] = len(m.Parameters)
	m.Parameters = append(m.Parameters, param)
}

func BoolPtr(b bool) *bool {
	return &b
}

func (m *ViewMeta) ParamByName(name string) (*Parameter, bool) {
	index, ok := m.index[name]
	if !ok {
		return nil, false
	}

	return m.Parameters[index], true
}

func (m *ViewMeta) SetVariables(variables map[string]bool) {
	m.variables = variables
}
