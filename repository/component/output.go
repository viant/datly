package component

import (
	"fmt"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"github.com/viant/xdatly/handler/response"
	"reflect"
)

type Output struct {
	Cardinality state.Cardinality    `json:",omitempty"`
	CaseFormat  formatter.CaseFormat `json:",omitempty"`
	OmitEmpty   bool                 `json:",omitempty"`
	Style       Style                `json:",omitempty"`

	//Filed defines optional main view data holder
	Field            string `json:",omitempty"`
	Exclude          []string
	NormalizeExclude *bool

	RevealMetric *bool
	DebugKind    view.MetaKind

	DataFormat string `json:",omitempty"` //default data format

	ResponseBody *BodySelector

	Type state.Type

	_caser    *format.Case
	_excluded map[string]bool
}

func (o *Output) Init() (err error) {
	if err = o.initCaser(); err != nil {
		return err
	}
	o.initExclude()
	return nil
}

func (o *Output) ShouldNormalizeExclude() bool {
	return o.NormalizeExclude == nil || *o.NormalizeExclude
}

func (o *Output) initExclude() {
	o._excluded = map[string]bool{}
	for _, excluded := range o.Exclude {
		o._excluded[excluded] = true
	}

	if !o.ShouldNormalizeExclude() {
		return
	}
	aBool := false
	o.NormalizeExclude = &aBool
	for i, excluded := range o.Exclude {
		o.Exclude[i] = formatter.NormalizePath(excluded)
	}

}

func (o *Output) initCaser() error {
	if o._caser != nil {
		return nil
	}

	if o.CaseFormat == "" {
		o.CaseFormat = formatter.UpperCamel
	}

	var err error
	caser, err := o.CaseFormat.Caser()
	if err != nil {
		return err
	}
	o._caser = &caser
	return nil
}

func (o *Output) Excluded() map[string]bool {
	return o._excluded
}

func (o *Output) FormatCase() *format.Case {
	return o._caser
}

func (c *Contract) DefaultOutputParameters(isReader bool, aView *view.View) (state.Parameters, error) {
	var parameters state.Parameters
	if isReader && c.Output.Style == ComprehensiveStyle {
		parameters = state.Parameters{
			DataOutputParameter(c.Output.Field),
			DefaultStatusOutputParameter(),
		}
		if aView != nil && aView.MetaTemplateEnabled() && aView.Template.Summary.Kind == view.MetaTypeRecord {
			parameters = append(parameters, state.NewParameter(aView.Template.Summary.Name,
				state.NewOutputLocation("Summary"),
				state.WithParameterType(aView.Template.Summary.Schema.Type())))
		}
		return parameters, nil
	}

	if c.Output.ResponseBody != nil && c.Output.ResponseBody.StateValue != "" {
		inputParameter := c.Input.Type.Parameters.Lookup(c.Output.ResponseBody.StateValue)
		if inputParameter == nil {
			return nil, fmt.Errorf("failed to lookup state value: %s", c.Output.ResponseBody.StateValue)
		}
		name := inputParameter.In.Name
		tag := ""
		if name == "" { //embed
			tag = `anonymous:"true"`
			name = c.Output.ResponseBody.StateValue
		}
		parameters = state.Parameters{
			{Name: name, In: state.NewState(c.Output.ResponseBody.StateValue), Schema: inputParameter.Schema, Tag: tag},
		}
		if inputParameter.In.Name != "" {
			parameters = append(parameters, &state.Parameter{Name: "Status", In: state.NewOutputLocation("status"), Tag: `anonymous:"true"`})
		}
	}
	return parameters, nil
}

func EnsureOutputParameterTypes(parameters []*state.Parameter, aView *view.View) {
	for _, parameter := range parameters {
		EnsureOutputParameterType(parameter, aView)
	}
}

func EnsureOutputParameterType(parameter *state.Parameter, aView *view.View) {
	rType := parameter.Schema.Type()
	if rType != nil && rType.Kind() != reflect.String {
		return
	}
	switch parameter.In.Kind {
	case state.KindOutput:
		switch parameter.In.Name {
		case "data":
			parameter.Schema = state.NewSchema(aView.OutputType())
		case "sql":
			parameter.Schema = state.NewSchema(reflect.TypeOf(""))
		case "status":
			parameter.Schema = state.NewSchema(reflect.TypeOf(response.Status{}))
			if parameter.Tag == "" {
				parameter.Tag = ` anonymous:"true"`
			}
		case "summary":
			parameter.Schema = aView.Template.Summary.Schema
		case "filter":
			predicateType := aView.Template.Parameters.PredicateStructType()
			parameter.Schema = state.NewSchema(predicateType)
		}
	}
}

func DefaultDataOutputParameter() *state.Parameter {
	return &state.Parameter{Name: "Output", Tag: `anonymous:"true"`, In: state.NewOutputLocation("data"), Schema: state.NewSchema(nil)}
}

func DataOutputParameter(name string) *state.Parameter {
	return &state.Parameter{Name: name, In: state.NewOutputLocation("data"), Schema: state.NewSchema(nil)}
}

func DefaultStatusOutputParameter() *state.Parameter {
	return &state.Parameter{Name: "Status", In: state.NewOutputLocation("status"), Tag: ` anonymous:"true"`, Schema: state.NewSchema(reflect.TypeOf(response.Status{}))}
}
