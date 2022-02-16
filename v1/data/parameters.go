package data

import "fmt"

//Parameters represents Parameters indexed by Parameter.Name
type Parameters map[string]*Parameter

//ParametersSlice represents slice of parameters
type ParametersSlice []*Parameter

//Index indexes parameters by Parameter.Name
func (p ParametersSlice) Index() Parameters {
	result := make(map[string]*Parameter)

	for parameterIndex := range p {
		keys := KeysOf(p[parameterIndex].Name, false)

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
