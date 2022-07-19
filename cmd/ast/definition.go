package ast

type (
	ViewMeta struct {
		Parameters     []*Parameter      `json:",omitempty" yaml:",omitempty"`
		Source         string            `json:",omitempty" yaml:",omitempty"`
		From           string            `json:",omitempty" yaml:",omitempty"`
		Expressions    []string          `json:",omitempty" yaml:",omitempty"`
		ParameterTypes map[string]string `json:",omitempty" yaml:",omitempty"`
		index          map[string]int
	}

	Parameter struct {
		Id       string `json:",omitempty" yaml:",omitempty"`
		Name     string `json:",omitempty" yaml:",omitempty"`
		Kind     string `json:",omitempty" yaml:",omitempty"`
		Required bool   `json:",omitempty" yaml:",omitempty"`
		Type     string `json:",omitempty" yaml:",omitempty"`
		fullName string
		Assumed  bool
		Typer    Typer `json:",omitempty" yaml:",omitempty"`
	}
)

func (m *ViewMeta) addParameter(param *Parameter) {
	if index, ok := m.index[param.Id]; ok {
		parameter := m.Parameters[index]
		parameter.Required = parameter.Required || param.Required
		if parameter.Assumed {
			parameter.Type = param.Type
		}

		return
	}

	m.index[param.Id] = len(m.Parameters)
	m.Parameters = append(m.Parameters, param)
}

func (m *ViewMeta) ParamByName(name string) (*Parameter, bool) {
	index, ok := m.index[name]
	if !ok {
		return nil, false
	}

	return m.Parameters[index], true
}
