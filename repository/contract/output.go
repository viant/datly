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
	Cardinality      state.Cardinality `json:",omitempty"`
	CaseFormat       text.CaseFormat   `json:",omitempty"`
	OmitEmpty        bool              `json:",omitempty"`
	Title            string            `json:",omitempty"`
	Exclude          []string
	NormalizeExclude *bool
	DebugKind        view.MetaKind
	DataFormat       string `json:",omitempty"` //default data format
	RevealMetric     *bool
	Type             state.Type
	ViewType         string
	_excluded        map[string]bool
}

func (o *Output) GetTitle() string {
	replcaer := data.NewMap()
	replcaer.Put("Time", time.Now().Format(time.RFC3339))
	replcaer.Put("DateTime", time.Now().Format("2006-01-02T15_04_05"))
	replcaer.Put("UnixTime", time.Now().Unix())
	return replcaer.ExpandAsText(o.Title)
}

func (o *Output) Init(ctx context.Context, aView *view.View, inputType *state.Type, isReader bool) (err error) {
	if err = o.ensureCaseFormat(); err != nil {
		return err
	}
	o.initExclude()
	o.addExcludePrefixesIfNeeded()
	o.initDebugStyleIfNeeded()
	if err = o.initParameters(aView, inputType, isReader); err != nil {
		return err
	}

	if isReader {
		if o.Type.IsAnonymous() {
			o.Type.Schema = state.NewSchema(aView.OutputType())
			return
		}
	}
	pkg := pkgPath
	if o.Type.Schema != nil && o.Type.Package != "" {
		pkg = o.Type.Package
	}
	o.Type.Parameters.FlagOutput()
	var options = []state.Option{
		state.WithResource(aView.Resource()),
		state.WithPackage(pkg),
	}
	if embeder := aView.GetResource().FSEmbedder; embeder != nil && embeder.EmbedFS() != nil {
		options = append(options, state.WithFS(embeder.EmbedFS()))
	}

	if err = o.Type.Init(options...); err != nil {
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

func (o *Output) IsExcluded(path string) bool {
	if len(o._excluded) == 0 {
		return false
	}
	if _, ok := o._excluded[path]; ok {
		return true
	}
	parts := strings.Split(path, ".")
	for i := 2; i < len(parts); i++ {
		key := strings.Join(parts[len(parts)-2:], ".")
		if _, ok := o._excluded[key]; ok {
			return ok
		}
	}

	return false
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

func (r *Output) Field() string {
	if r.Type.IsAnonymous() {
		return ""
	}
	outputParameter := r.Type.Parameters.LookupByLocation(state.KindOutput, outputkeys.ViewData)
	if outputParameter == nil {
		if candidate := r.Type.Parameters.LookupByLocation(state.KindRequestBody, ""); candidate != nil {
			outputParameter = candidate
		}
	}
	if outputParameter != nil {
		return outputParameter.Name
	}
	return "data"
}

func (r *Output) addExcludePrefixesIfNeeded() {
	field := r.Field()
	if field == "" {
		return
	}
	for i, actual := range r.Exclude {
		if strings.HasPrefix(actual, field) {
			continue
		}
		r.Exclude[i] = field + "." + actual
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

func (o *Output) initParameters(aView *view.View, bodyType *state.Type, isReader bool) (err error) {
	if bodyParameter := o.Type.Parameters.LookupByLocation(state.KindRequestBody, ""); bodyParameter != nil {
		schema := bodyParameter.Schema
		if schema.Name == "" && schema.Type() == nil {
			schema = bodyType.Schema
		}
	}

	if len(o.Type.Parameters) == 0 {
		o.Type.Parameters, err = o.defaultParameters(aView, bodyType.Parameters, isReader)
	}
	return EnsureParameterTypes(o.Type.Parameters, aView)
}

func (o *Output) defaultParameters(aView *view.View, inputParameters state.Parameters, isReader bool) (state.Parameters, error) {
	var parameters state.Parameters
	if isReader {
		dataParameter := DataOutputParameter(outputkeys.ViewData)
		dataParameter.Tag = `anonynous:"true"`
		return state.Parameters{dataParameter}, nil
	}
	return parameters, nil
}

// EnsureParameterTypes update output kind parameter type
func EnsureParameterTypes(parameters []*state.Parameter, aView *view.View) error {
	for _, parameter := range parameters {
		if err := ensureParameterType(parameter, aView); err != nil {
			return err
		}

		if len(parameter.Object) > 0 {
			EnsureParameterTypes(parameter.Object, aView)
		}
		if len(parameter.Repeated) > 0 {
			EnsureParameterTypes(parameter.Repeated, aView)
		}
	}
	return nil
}

func ensureParameterType(parameter *state.Parameter, aView *view.View) error {
	rType := parameter.Schema.Type()
	if rType != nil && rType.Kind() != reflect.String && rType.Kind() != reflect.Interface {
		return nil
	}

	switch parameter.In.Kind {
	case state.KindOutput:
		key := strings.ToLower(parameter.In.Name)
		switch key {
		case "":
			return nil
		case outputkeys.ViewData:
			if aView != nil {
				parameter.Schema = state.NewSchema(aView.OutputType())
			}

		case outputkeys.ViewSummaryData:
			if aView != nil {
				if aView.Template.Summary == nil {
					return fmt.Errorf("failed to lookup summary view for: %s", aView.Name)
				}
				parameter.Schema = aView.Template.Summary.Schema
			}
		case outputkeys.Filter:
			if aView != nil {
				predicateType := aView.Template.Parameters.PredicateStructType()
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
	return nil
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
