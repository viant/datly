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
	result := make(map[string]*Parameter)

	for parameterIndex := range p {
		keys := shared.KeysOf(p[parameterIndex].Name, false)

		for _, key := range keys {
			result[key] = p[parameterIndex]
		}
	}

	return result
}

//Filter filters ParametersSlice with given Kind and creates Parameters
//Uses shared.KeysOf
func (p ParametersSlice) Filter(kind Kind) Parameters {
	result := make(map[string]*Parameter)

	for parameterIndex := range p {
		if p[parameterIndex].In.Kind != kind {
			continue
		}

		keys := shared.KeysOf(p[parameterIndex].In.Name, false)

		for _, key := range keys {
			result[key] = p[parameterIndex]
		}
	}

	return result
}

//Lookup returns Parameter with given name
func (p Parameters) Lookup(paramName string) (*Parameter, error) {
	if param, ok := p[paramName]; ok {
		return param, nil
	}

	return nil, fmt.Errorf("not found parameter %v", paramName)

}
