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
	}
)

func (m *ViewMeta) addParameter(param *Parameter) {
	if index, ok := m.index[param.Id]; ok {
		m.Parameters[index].Required = m.Parameters[index].Required || param.Required
		return
	}

	m.index[param.Id] = len(m.Parameters)
	m.Parameters = append(m.Parameters, param)
}
