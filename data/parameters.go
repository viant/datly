package data

import (
	"fmt"
	"github.com/viant/datly/shared"
)

//Parameters represents Parameter map indexed by Parameter.Name
type Parameters map[string]*Parameter

//ParametersSlice represents slice of parameters
type ParametersSlice []*Parameter

//Index indexes parameters by Parameter.Name
//Uses shared.KeysOf
func (p ParametersSlice) Index() Parameters {
	result := Parameters(make(map[string]*Parameter))

	for parameterIndex := range p {
		result.Register(p[parameterIndex])
	}

	return result
}

//Filter filters ParametersSlice with given Kind and creates Parameters
func (p ParametersSlice) Filter(kind Kind) Parameters {
	result := make(map[string]*Parameter)

	for parameterIndex := range p {
		if p[parameterIndex].In.Kind != kind {
			continue
		}
		result[p[parameterIndex].In.Name] = p[parameterIndex]

	}

	return result
}

func (p Parameters) merge(with Parameters) {
	for s := range with {
		p[s] = with[s]
	}
}

//Lookup returns Parameter with given name
func (p Parameters) Lookup(paramName string) (*Parameter, error) {
	if param, ok := p[paramName]; ok {
		return param, nil
	}

	return nil, fmt.Errorf("not found parameter %v", paramName)
}

//Register registers parameter
//Uses shared.KeysOf
func (p Parameters) Register(parameter *Parameter) {
	keys := shared.KeysOf(parameter.Name, false)

	for _, key := range keys {
		p[key] = parameter
	}
}
