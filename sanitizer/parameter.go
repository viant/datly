package sanitizer

type ParameterHint struct {
	Parameter string
	Hint      string
}

func NewParameterHint(name, hint string) *ParameterHint {
	return &ParameterHint{
		Parameter: name,
		Hint:      hint,
	}
}

type ParameterHints []*ParameterHint

func (p *ParameterHints) Index() map[string]*ParameterHint {
	var result = make(map[string]*ParameterHint)
	for i, item := range *p {
		result[item.Parameter] = (*p)[i]
	}

	return result
}
