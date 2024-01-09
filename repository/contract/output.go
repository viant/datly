package contract

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository/content"
	asynckeys "github.com/viant/datly/repository/locator/async/keys"
	metakeys "github.com/viant/datly/repository/locator/meta/keys"
	outputkeys "github.com/viant/datly/repository/locator/output/keys"

	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	"github.com/viant/toolbox/data"
	"github.com/viant/xdatly/handler/response"
	"net/url"
	"reflect"
	"strings"
	"time"
)

const (
	FormatQuery = "_format"
)

type Output struct {
	Cardinality state.Cardinality `json:",omitempty"`
	CaseFormat  text.CaseFormat   `json:",omitempty"`
	OmitEmpty   bool              `json:",omitempty"`
	Style       Style             `json:",omitempty"`
	Title       string            `json:",omitempty"`
	//Filed defines optional main view data holder
	//deprecated
	Field            string `json:",omitempty"`
	Exclude          []string
	NormalizeExclude *bool

	DebugKind view.MetaKind

	DataFormat string `json:",omitempty"` //default data format

	ResponseBody *BodySelector
	RevealMetric *bool
	Type         state.Type
	Doc          state.Documentation
	FilterDoc    state.Documentation
	_excluded    map[string]bool
}

func (o *Output) GetTitle() string {
	replcaer := data.NewMap()
	replcaer.Put("Time", time.Now().Format(time.RFC3339))
	replcaer.Put("DateTime", time.Now().Format("2006-01-02T15_04_05"))
	replcaer.Put("UnixTime", time.Now().Unix())
	return replcaer.ExpandAsText(o.Title)
}

func (o *Output) Init(ctx context.Context, aView *view.View, inputParameters state.Parameters, isReader bool) (err error) {
	if err = o.ensureCaseFormat(); err != nil {
		return err
	}
	o.initExclude()
	o.addExcludePrefixesIfNeeded()
	o.initDebugStyleIfNeeded()
	if err = o.initParameters(aView, inputParameters, o.Doc, isReader); err != nil {
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
			o.Field = "ViewData"

		} else {
			o.Field = "ResponseBody"
		}
	}
	pkg := pkgPath
	if o.Type.Package != "" {
		pkg = o.Type.Package
	}
	o.Type.Parameters.FlagOutput()
	if err = o.Type.Init(state.WithResource(aView.Resource()), state.WithPackage(pkg)); err != nil {
		return fmt.Errorf("failed to initialise output: %w", err)
	}

	return nil
}

func (o *Output) ContentType(format string) string {
	switch format {
	case content.CSVFormat:
		return content.CSVContentType
	case content.XLSFormat:
		return content.XLSContentType
	case content.XMLFormat:
		return content.XMLContentType
	default:
		return content.JSONContentType
	}
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

func (o *Output) ensureCaseFormat() error {
	if o.CaseFormat == "" {
		o.CaseFormat = text.CaseFormatUpperCamel
	}
	return nil
}

func (o *Output) Excluded() map[string]bool {
	return o._excluded
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
	if o.DebugKind != view.MetaKindRecord {
		o.DebugKind = view.MetaKindHeader
	}
}

func (o *Output) initParameters(aView *view.View, inputParameters state.Parameters, doc state.Documentation, isReader bool) (err error) {
	if o.Type.IsAnonymous() {
		o.Style = BasicStyle
	} else if outputParameters := o.Type.Parameters.Filter(state.KindOutput); len(outputParameters) > 0 {
		o.Style = ComprehensiveStyle
		for _, dataParameter := range outputParameters {
			if dataParameter.In.Name == outputkeys.ViewData {
				o.Field = dataParameter.Name
			}
		}
	}
	if len(o.Type.Parameters) == 0 {
		o.Type.Parameters, err = o.defaultParameters(aView, inputParameters, isReader)
	}
	EnsureParameterTypes(o.Type.Parameters, aView, doc, o.FilterDoc)
	return err
}

func (o *Output) defaultParameters(aView *view.View, inputParameters state.Parameters, isReader bool) (state.Parameters, error) {
	var parameters state.Parameters
	if isReader {
		if o.Style == ComprehensiveStyle {
			parameters = state.Parameters{
				DataOutputParameter(o.Field),
				DefaultStatusOutputParameter(),
			}
			if aView != nil && aView.MetaTemplateEnabled() && aView.Template.Summary.Kind == view.MetaKindRecord {
				parameters = append(parameters, state.NewParameter(aView.Template.Summary.Name,
					state.NewOutputLocation("summary"),
					state.WithParameterType(aView.Template.Summary.Schema.Type())))
			}
			return parameters, nil
		}
		dataParameter := DataOutputParameter(outputkeys.ViewData)
		dataParameter.Tag = `anonynous:"true"`
		return state.Parameters{dataParameter}, nil
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

// EnsureParameterTypes update output kind parameter type
func EnsureParameterTypes(parameters []*state.Parameter, aView *view.View, doc state.Documentation, filterDoc state.Documentation) {
	for _, parameter := range parameters {
		ensureParameterType(parameter, aView, doc, filterDoc)
		var paramDoc state.Documentation
		if doc != nil {
			paramDoc, _ = doc.FieldDocumentation(parameter.Name)
			paramDescription, ok := doc.ByName(parameter.Name)
			if ok {
				parameter.Description = paramDescription
			}
		}

		if len(parameter.Object) > 0 {
			EnsureParameterTypes(parameter.Object, aView, paramDoc, filterDoc)
		}
		if len(parameter.Repeated) > 0 {
			EnsureParameterTypes(parameter.Repeated, aView, paramDoc, filterDoc)
		}
	}
}

func ensureParameterType(parameter *state.Parameter, aView *view.View, doc state.Documentation, filterDoc state.Documentation) {
	rType := parameter.Schema.Type()
	if rType != nil && rType.Kind() != reflect.String && rType.Kind() != reflect.Interface {
		return
	}

	switch parameter.In.Kind {
	case state.KindOutput:
		key := strings.ToLower(parameter.In.Name)
		switch key {
		case "":
			return
		case outputkeys.ViewData:
			if aView != nil {
				parameter.Schema = state.NewSchema(aView.OutputType())
			}

		case outputkeys.ViewSummaryData:
			if aView != nil {
				parameter.Schema = aView.Template.Summary.Schema
			}
		case outputkeys.Filter:
			if aView != nil {
				predicateType := aView.Template.Parameters.PredicateStructType(filterDoc)
				parameter.Schema = state.NewSchema(predicateType)
				parameter.Schema.Name = strings.Title(aView.Name) + "Filter"
				parameter.SetTypeNameTag()
			} else {
				parameter.Schema.Name = "Filter"
				parameter.Schema.DataType = "Filter"
			}
		default:
			//static types
			UpdateOutputParameterType(parameter)

		}
	case state.KindMeta:
		UpdateParameterMetaType(parameter)
	case state.KindAsync:
		UpdateParameterAsyncType(parameter)
	}
}

func UpdateParameterAsyncType(parameter *state.Parameter) {
	key := strings.ToLower(parameter.In.Name)
	switch key {
	default:
		if rType, ok := asynckeys.Types[key]; ok {
			updateParameterType(parameter, rType)
		}
	}
}

func UpdateParameterMetaType(parameter *state.Parameter) {
	key := strings.ToLower(parameter.In.Name)
	switch key {
	default:
		if rType, ok := metakeys.Types[key]; ok {
			updateParameterType(parameter, rType)
		}
	}
}

func UpdateOutputParameterType(parameter *state.Parameter) {
	key := strings.ToLower(parameter.In.Name)
	if rType, ok := outputkeys.Types[key]; ok {
		updateParameterType(parameter, rType)
	}
}

func updateParameterType(parameter *state.Parameter, rType reflect.Type) {
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

func DefaultDataOutputParameter() *state.Parameter {
	return &state.Parameter{Name: "Output", Tag: `anonymous:"true"`, In: state.NewOutputLocation(outputkeys.ViewData), Schema: state.NewSchema(nil)}
}

func DataOutputParameter(name string) *state.Parameter {
	return &state.Parameter{Name: name, In: state.NewOutputLocation(outputkeys.ViewData), Schema: state.NewSchema(nil)}
}

func DefaultStatusOutputParameter() *state.Parameter {
	return &state.Parameter{Name: "Status", In: state.NewOutputLocation("status"), Tag: ` anonymous:"true"`, Schema: state.NewSchema(reflect.TypeOf(response.Status{}))}
}
