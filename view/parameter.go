package view

import (
	"fmt"
	"github.com/viant/datly/shared"
)

//ParametersIndex represents Parameter map indexed by Parameter.Name
type ParametersIndex map[string]*Parameter

//ParametersSlice represents slice of parameters
type ParametersSlice []*Parameter

//Index indexes parameters by Parameter.Name
func (p ParametersSlice) Index() ParametersIndex {
	result := ParametersIndex(make(map[string]*Parameter))
	for parameterIndex := range p {
		result.Register(p[parameterIndex])
	}

	return result
}

//Filter filters ParametersSlice with given Kind and creates Template
func (p ParametersSlice) Filter(kind Kind) ParametersIndex {
	result := make(map[string]*Parameter)

	for parameterIndex := range p {
		if p[parameterIndex].In.Kind != kind {
			continue
		}
		result[p[parameterIndex].In.Name] = p[parameterIndex]

	}

	return result
}

func (p ParametersIndex) merge(with ParametersIndex) {
	for s := range with {
		p[s] = with[s]
	}
}

//Lookup returns Parameter with given name
func (p ParametersIndex) Lookup(paramName string) (*Parameter, error) {

	if param, ok := p[paramName]; ok {
		return param, nil
	}

	return nil, fmt.Errorf("not found parameter %v", paramName)
}

//Register registers parameter
func (p ParametersIndex) Register(parameter *Parameter) {
	keys := shared.KeysOf(parameter.Name, false)
	for _, key := range keys {
		p[key] = parameter
	}
}
