package option

type ParameterHint struct {
	Parameter string
	Hint      string
}

type ParameterHints []*ParameterHint

func (p *ParameterHints) Index() map[string]*ParameterHint {
	var result = make(map[string]*ParameterHint)
	for i, item := range *p {
		result[item.Parameter] = (*p)[i]
	}

	return result
}
