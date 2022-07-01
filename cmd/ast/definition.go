package ast

type (
	ViewMeta struct {
		Parameters     []*Parameter `json:",omitempty" yaml:",omitempty"`
		Source         string       `json:",omitempty" yaml:",omitempty"`
		From           string       `json:",omitempty" yaml:",omitempty"`
		HasVeltySyntax bool         `json:",omitempty" yaml:",omitempty"`

		index               map[string]int
		actualParametersLen int
	}

	Parameter struct {
		Id        string `json:",omitempty" yaml:",omitempty"`
		Name      string `json:",omitempty" yaml:",omitempty"`
		Kind      string `json:",omitempty" yaml:",omitempty"`
		Required  bool   `json:",omitempty" yaml:",omitempty"`
		Type      string `json:",omitempty" yaml:",omitempty"`
		Positions []int  `json:",omitempty" yaml:",omitempty"`
		fullName  string
	}
)

func (m *ViewMeta) addParameter(param *Parameter, indexPosition bool) {
	if indexPosition {
		m.actualParametersLen++
	}
	actualIndex := m.actualParametersLen - 1

	if index, ok := m.index[param.Id]; ok {
		if !indexPosition {
			return
		}

		m.Parameters[index].Positions = append(m.Parameters[index].Positions, actualIndex)
		return
	}

	m.index[param.Id] = len(m.Parameters)
	if indexPosition {
		param.Positions = append(param.Positions, actualIndex)
	}

	m.Parameters = append(m.Parameters, param)
}
