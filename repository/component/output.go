package component

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"github.com/viant/xdatly/handler/response"
	"net/url"
	"reflect"
	"strings"
)

const (
	FormatQuery = "_format"
)

type Output struct {
	Cardinality state.Cardinality    `json:",omitempty"`
	CaseFormat  formatter.CaseFormat `json:",omitempty"`
	OmitEmpty   bool                 `json:",omitempty"`
	Style       Style                `json:",omitempty"`

	//Filed defines optional main view data holder
	//deprecated
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

func (o *Output) Init(ctx context.Context, aView *view.View, inputParameters state.Parameters, isReader bool) (err error) {
	if err = o.initCaser(); err != nil {
		return err
	}
	o.initExclude()
	o.addExcludePrefixesIfNeeded()
	o.initDebugStyleIfNeeded()
	if err = o.initParameters(aView, inputParameters, isReader); err != nil {
		return err
	}
	if (o.Style == "" || o.Style == BasicStyle) && o.Field == "" {
		o.Style = BasicStyle
		if isReader {
			o.Type.Schema = state.NewSchema(aView.OutputType())
			return nil
		}
	}
	if o.Field == "" && o.Style != BasicStyle {
		if isReader {
			o.Field = "Data"

		} else {
			o.Field = "ResponseBody"
		}
	}
	if o.Type.Schema != nil && o.Type.Name != "" {
		lookupType := aView.Resource().LookupType()
		rType, err := lookupType(o.Type.Name)
		if err != nil {
			return fmt.Errorf("unknwout output: %w", err)
		}
		o.Type.SetType(rType)

	}

	o.Type.Parameters.FlagOutput()
	if err = o.Type.Init(state.WithResource(aView.Resource()), state.WithPackage(pkgPath)); err != nil {
		return fmt.Errorf("failed to initialise output: %w", err)
	}

	return nil
}

func (o *Output) Format(query url.Values) string {
	outputFormat := query.Get(FormatQuery)
	if outputFormat == "" {
		outputFormat = o.DataFormat
	}
	if outputFormat == "" {
		outputFormat = content.JSONFormat
	}
	return outputFormat
}

func (o *Output) IsRevealMetric() bool {
	if o.RevealMetric == nil {
		return false
	}
	return *o.RevealMetric
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

func (r *Output) addExcludePrefixesIfNeeded() {
	if r.Field == "" {
		return
	}
	for i, actual := range r.Exclude {
		r.Exclude[i] = r.Field + "." + actual
	}
}

func (o *Output) initDebugStyleIfNeeded() {
	if o.RevealMetric == nil || !*o.RevealMetric {
		return
	}
	if o.DebugKind != view.MetaTypeRecord {
		o.DebugKind = view.MetaTypeHeader
	}
}

func (o *Output) initParameters(aView *view.View, inputParameters state.Parameters, isReader bool) (err error) {
	if o.Type.IsAnonymous() {
		o.Style = BasicStyle
	} else if outputParameters := o.Type.Parameters.Filter(state.KindOutput); outputParameters != nil {
		o.Style = ComprehensiveStyle
		for _, dataParameter := range outputParameters {
			if dataParameter.In.Name == "data" {
				o.Field = dataParameter.Name
			}
		}
	}
	if len(o.Type.Parameters) == 0 {
		o.Type.Parameters, err = o.defaultParameters(aView, inputParameters, isReader)
	}
	EnsureOutputKindParameterTypes(o.Type.Parameters, aView)
	return err
}

func (o *Output) defaultParameters(aView *view.View, inputParameters state.Parameters, isReader bool) (state.Parameters, error) {
	var parameters state.Parameters
	if isReader && o.Style == ComprehensiveStyle {
		parameters = state.Parameters{
			DataOutputParameter(o.Field),
			DefaultStatusOutputParameter(),
		}
		if aView != nil && aView.MetaTemplateEnabled() && aView.Template.Summary.Kind == view.MetaTypeRecord {
			parameters = append(parameters, state.NewParameter(aView.Template.Summary.Name,
				state.NewOutputLocation("Summary"),
				state.WithParameterType(aView.Template.Summary.Schema.Type())))
		}
		return parameters, nil
	}

	if o.ResponseBody != nil && o.ResponseBody.StateValue != "" {
		inputParameter := inputParameters.Lookup(o.ResponseBody.StateValue)
		if inputParameter == nil {
			return nil, fmt.Errorf("failed to lookup state value: %s", o.ResponseBody.StateValue)
		}
		name := inputParameter.In.Name
		tag := ""
		if name == "" { //embed
			tag = `anonymous:"true"`
			name = o.ResponseBody.StateValue
		}
		parameters = state.Parameters{
			{Name: name, In: state.NewState(o.ResponseBody.StateValue), Schema: inputParameter.Schema, Tag: tag},
		}
		if inputParameter.In.Name != "" {
			parameters = append(parameters, &state.Parameter{Name: "Status", In: state.NewOutputLocation("status"), Tag: `anonymous:"true"`})
		}
	}
	return parameters, nil
}

// EnsureOutputKindParameterTypes update output kind parameter type
func EnsureOutputKindParameterTypes(parameters []*state.Parameter, aView *view.View) {
	for _, parameter := range parameters {
		ensureOutputParameterType(parameter, aView)
		if len(parameter.Group) > 0 {
			EnsureOutputKindParameterTypes(parameter.Group.FilterByKind(state.KindOutput), aView)
		}
		if len(parameter.Repeated) > 0 {
			EnsureOutputKindParameterTypes(parameter.Repeated.FilterByKind(state.KindOutput), aView)
		}
	}
}

func ensureOutputParameterType(parameter *state.Parameter, aView *view.View) {
	rType := parameter.Schema.Type()
	if rType != nil && rType.Kind() != reflect.String && rType.Kind() != reflect.Interface {
		return
	}
	if parameter.In.Kind == state.KindOutput {
		key := strings.ToLower(parameter.In.Name)
		switch key {
		case "":
			return
		case "data":
			if aView != nil {
				parameter.Schema = state.NewSchema(aView.OutputType())
			}

		case "summary":
			if aView != nil {
				parameter.Schema = aView.Template.Summary.Schema
			}
		case "filter":
			if aView != nil {
				predicateType := aView.Template.Parameters.PredicateStructType()
				parameter.Schema = state.NewSchema(predicateType)
			} else {
				parameter.Schema.Name = "Filter"
				parameter.Schema.DataType = "Filter"
			}
		default:
			//static types
			if rType, ok := keys.Types[key]; ok {
				parameter.Schema = state.NewSchema(rType)
				if rType.Kind() == reflect.Ptr {
					rType = rType.Elem()
				}
				if rType.Kind() == reflect.Struct {

					if parameter.Name == "" {
						parameter.Name = rType.Name()
					}
					if parameter.Tag == "" {
						parameter.Tag = `json:",omitempty"`
						if parameter.Name == rType.Name() {
							parameter.Tag += ` anonymous:"true"`
						}
					}
				}
			}
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
