package ast

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
	}

	Parameter struct {
		Id            string `json:",omitempty" yaml:",omitempty"`
		Name          string `json:",omitempty" yaml:",omitempty"`
		Kind          string `json:",omitempty" yaml:",omitempty"`
		Required      *bool  `json:",omitempty" yaml:",omitempty"`
		DataType      string `json:",omitempty" yaml:",omitempty"`
		DerivedColumn string `json:",omitempty" yaml:",omitempty"`
		Repeated      bool   `json:",omitempty" yaml:",omitempty"`
		fullName      string
		Assumed       bool
		Typer         Typer  `json:",omitempty" yaml:",omitempty"`
		SQL           string `json:",omitempty" yaml:",omitempty"`
	}
)

func (m *ViewMeta) addParameter(param *Parameter) {
	if index, ok := m.index[param.Id]; ok {
		parameter := m.Parameters[index]
		parameter.Required = boolPtr((parameter.Required != nil && *parameter.Required) || (param.Required != nil && *param.Required))
		if parameter.Assumed {
			parameter.DataType = param.DataType
		}

		return
	}

	m.index[param.Id] = len(m.Parameters)
	m.Parameters = append(m.Parameters, param)
}

func boolPtr(b bool) *bool {
	return &b
}

func (m *ViewMeta) ParamByName(name string) (*Parameter, bool) {
	index, ok := m.index[name]
	if !ok {
		return nil, false
	}

	return m.Parameters[index], true
}
