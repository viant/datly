package cmd

import (
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/config"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"strings"
)

type ParametersIndex struct {
	parameterKinds map[string]view.Kind
	parameters     map[string]*view.Parameter
	consts         map[string]interface{}
	types          map[string]string
	hints          map[string]*sanitize.ParameterHint
	utilsIndex     map[string]bool
	paramsMeta     map[string]*Parameter
}

func NewParametersIndex(routeConfig *option.RouteConfig, hints map[string]*sanitize.ParameterHint) *ParametersIndex {
	index := &ParametersIndex{
		utilsIndex:     map[string]bool{},
		parameterKinds: map[string]view.Kind{},
		parameters:     map[string]*view.Parameter{},
		types:          map[string]string{},
		consts:         map[string]interface{}{},
		hints:          map[string]*sanitize.ParameterHint{},
		paramsMeta:     map[string]*Parameter{},
	}

	if routeConfig != nil {
		index.AddConsts(routeConfig.Const)
		index.AddUriParams(extractURIParams(routeConfig.URI))
	}

	if hints != nil {
		index.AddHints(hints)
	}

	return index
}

func (p *ParametersIndex) AddUriParams(params map[string]bool) {
	for paramName := range params {
		p.parameterKinds[paramName] = view.KindPath
	}
}

func (p *ParametersIndex) AddDataViewParam(paramName string) {
	p.parameterKinds[paramName] = view.KindDataView
}

func (p *ParametersIndex) AddParamParam(paramName string) {
	p.parameterKinds[paramName] = view.KindParam
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
		p.parameterKinds[key] = view.KindLiteral
	}
}

func (p *ParametersIndex) AddHints(hints map[string]*sanitize.ParameterHint) {
	for paramName := range hints {
		p.AddParamHint(paramName, hints[paramName])
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

func (p *ParametersIndex) ParamsMetaWithHint(paramName string, hint *sanitize.ParameterHint) (*Parameter, error) {
	parameter := p.ParamMeta(paramName)
	if hint == nil {
		return parameter, nil
	}

	jsonHint, SQL := sanitize.SplitHint(hint.Hint)

	if err := tryUnmarshalHint(jsonHint, parameter); err != nil {
		return nil, err
	}

	if hint.StructQLQuery != nil {
		parameter.Codec = config.CodecStructql

		if parameter.Required == nil {
			parameter.Required = boolPtr(false)
		}
		parameter.SQL = hint.StructQLQuery.SQL
		parameter.SQLCodec = true
		parameter.Location = &hint.StructQLQuery.Source
		parameter.Kind = string(view.KindParam)
	} else if isSQLLikeCodec(parameter.Codec) {
		parameter.SQL = SQL

		sqlUpperCased := strings.ToUpper(SQL)
		if !strings.Contains(sqlUpperCased, " LIMIT ") && strings.Contains(sqlUpperCased, "ARRAY_AGG") {
			parameter.SQL += " LIMIT 1"
		}
		parameter.SQLCodec = true
	} else if strings.TrimSpace(SQL) != "" {
		parameter.Kind = string(view.KindDataView)
		parameter.SQL = SQL
	}

	return parameter, nil
}

func (p *ParametersIndex) ParamMeta(paramName string) *Parameter {
	parameter, ok := p.paramsMeta[paramName]
	if !ok {
		parameter = &Parameter{
			ParameterConfig: option.ParameterConfig{
				Name: paramName,
			},
			Assumed: true,
		}
		p.paramsMeta[paramName] = parameter
	}
	return parameter
}

func (p *ParametersIndex) ParamsMetaWithComment(paramName, hint string) (*Parameter, error) {
	parameter := p.ParamMeta(paramName)
	if hint == "" {
		return parameter, nil
	}

	if err := tryUnmarshalHint(hint, parameter); err != nil {
		return nil, err
	}

	return parameter, nil
}

func (p *ParametersIndex) AddParamHint(name string, aHint *sanitize.ParameterHint) {
	p.hints[name] = aHint
	actualHint, _ := sanitize.SplitHint(aHint.Hint)
	actualHint = strings.TrimSpace(actualHint)

	paramMeta := &option.ParamMeta{}
	tryUnmrashalHintWithWarn(actualHint, &paramMeta)
	if paramMeta.Util {
		p.utilsIndex[name] = true
	}
}
