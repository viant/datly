package cmd

import (
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
)

type ParametersIndex struct {
	parameterKinds map[string]view.Kind
	parameters     map[string]*view.Parameter
	consts         map[string]interface{}
	types          map[string]string
	hints          map[string]*sanitize.ParameterHint
}

func NewParametersIndex() *ParametersIndex {
	return &ParametersIndex{
		parameterKinds: map[string]view.Kind{},
		parameters:     map[string]*view.Parameter{},
		types:          map[string]string{},
		consts:         map[string]interface{}{},
		hints:          map[string]*sanitize.ParameterHint{},
	}
}

func (p *ParametersIndex) AddUriParams(params map[string]bool) {
	for paramName := range params {
		p.parameterKinds[paramName] = view.PathKind
	}
}

func (p *ParametersIndex) AddDataViewParam(paramName string) {
	p.parameterKinds[paramName] = view.DataViewKind
}

func (p *ParametersIndex) ParamType(paramName string) (view.Kind, bool) {
	aKind, ok := p.parameterKinds[paramName]
	return aKind, ok
}

func (p *ParametersIndex) AddParamTypes(paramTypes map[string]string) {
	for paramName, paramType := range paramTypes {
		p.types[paramName] = paramType
	}
}

func (p *ParametersIndex) AddConsts(consts map[string]interface{}) {
	for key := range consts {
		p.consts[key] = consts[key]
		p.parameterKinds[key] = view.LiteralKind
	}
}

func (p *ParametersIndex) AddHints(hints map[string]*sanitize.ParameterHint) {
	for paramName := range hints {
		p.hints[paramName] = hints[paramName]
	}
}

func (p *ParametersIndex) Param(name string) (*view.Parameter, bool) {
	parameter, ok := p.parameters[name]
	if ok {
		return parameter, true
	}

	parameter = &view.Parameter{
		Name:   name,
		Schema: &view.Schema{},
	}
	p.parameters[name] = parameter
	return parameter, false
}

func (p *ParametersIndex) AddParameter(parameter *view.Parameter) {
	p.parameters[parameter.Name] = parameter
}
