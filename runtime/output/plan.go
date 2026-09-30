package output

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"reflect"
	"strings"
	"sync"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	jsonmarshal "github.com/viant/structology/encoding/json/marshal"
	"github.com/viant/tagly/format"
	"github.com/viant/tagly/format/text"
	ftime "github.com/viant/tagly/format/time"
	xshape "github.com/viant/x/shape"
)

type Marshaller interface{ Marshal(any) ([]byte, error) }

type CompileInput struct {
	Component *spec.Component
	Type      reflect.Type
	DataField string
}

type Compiler struct {
	Lookup func(string) (reflect.Type, error)
}

// Plan contains registered output authority, never invocation input or rows.
type Plan struct {
	typeOf          reflect.Type
	format          string
	formatSelector  *spec.BindSource
	caseFormat      text.CaseFormat
	timeLayout      string
	custom          Marshaller
	rows            *rowsPlan
	exclude         exclusions
	jsonEncoder     *structjson.Marshaller
	standardJSON    *jsonmarshal.Standard
	omitEmpty       bool
	title           string
	presentation    *presentation
	rowPresentation *presentation
	csvEncoder      cachedCSV
	xmlEncoders     sync.Map
}

type Result struct {
	Data               []byte
	ContentType        string
	ContentDisposition string
	Filename           string
}

func (c Compiler) Compile(input CompileInput) (*Plan, error) {
	p := &Plan{typeOf: input.Type, format: "json"}
	settings := (*spec.Settings)(nil)
	if input.Component != nil {
		settings = input.Component.Settings
		for _, parameter := range spec.EffectiveParameters(input.Component.Parameters) {
			if parameter == nil || !parameter.FormatSelector {
				continue
			}
			if p.formatSelector != nil {
				return nil, fmt.Errorf("output format selector is declared more than once")
			}
			if parameter.QuerySelector != nil {
				return nil, fmt.Errorf("one input cannot be both an output format and view query selector")
			}
			source := parameter.Source
			source.Kind = strings.ToLower(strings.TrimSpace(source.Kind))
			source.Name = strings.TrimSpace(source.Name)
			if !(source.Kind == "query" && source.Name != "") && !(source.Kind == "header" && strings.EqualFold(source.Name, "Accept")) {
				return nil, fmt.Errorf("output format selector requires a query source or header/Accept")
			}
			if parameter.TypeExpr != "" && parameter.TypeExpr != "string" {
				return nil, fmt.Errorf("output format selector requires a string type")
			}
			p.formatSelector = &source
		}
	}
	if settings != nil {
		if settings.Format != "" {
			p.format = strings.ToLower(strings.TrimSpace(settings.Format))
		}
		if settings.CaseFormat != "" {
			p.caseFormat = text.NewCaseFormat(settings.CaseFormat)
			if p.caseFormat == "" {
				return nil, fmt.Errorf("unsupported output case format %q", settings.CaseFormat)
			}
		}
		if settings.DateFormat != "" {
			p.timeLayout = ftime.DateFormatToTimeLayout(settings.DateFormat)
		}
		if settings.Output != nil {
			p.exclude = exclusions(append([]string(nil), settings.Output.Exclude...))
			p.omitEmpty = settings.Output.OmitEmpty
			p.title = settings.Output.Title
		}
		if name := strings.TrimSpace(settings.JSONMarshalType); name != "" {
			if c.Lookup == nil {
				return nil, fmt.Errorf("output marshaller %s requires type authority", name)
			}
			typeOf, err := c.Lookup(name)
			if err != nil {
				return nil, err
			}
			if typeOf == nil {
				return nil, fmt.Errorf("output marshaller %s was not found", name)
			}
			if typeOf.Kind() == reflect.Pointer {
				typeOf = typeOf.Elem()
			}
			instance := reflect.New(typeOf).Interface()
			marshaller, ok := instance.(Marshaller)
			if !ok {
				return nil, fmt.Errorf("output marshaller %s must implement Marshal(any) ([]byte,error)", name)
			}
			p.custom = marshaller
		}
	}
	if _, err := ContentType(p.format); err != nil {
		return nil, err
	}
	if p.JSONOnly() && p.format != "json" {
		return nil, fmt.Errorf("custom JSON output cannot use %s as its default format", p.format)
	}
	if p.formatSelector != nil && p.TransportReady() {
		return nil, fmt.Errorf("explicit Response output owns its media type and cannot use FormatSelector")
	}
	if input.Component != nil {
		for _, route := range input.Component.Routes {
			if route != nil && route.Marshaller != "" {
				if _, err := ContentType(route.Marshaller); err != nil {
					return nil, err
				}
				if p.JSONOnly() && !strings.EqualFold(route.Marshaller, "json") {
					return nil, fmt.Errorf("custom JSON output cannot use %s route format", route.Marshaller)
				}
			}
		}
	}
	var err error
	p.rows, err = compileRows(input)
	if err != nil {
		return nil, err
	}
	p.exclude, err = p.exclude.resolve(input.Type, p.rows)
	if err != nil {
		return nil, err
	}
	if p.custom == nil && p.transformedJSON() && p.typeOf != nil {
		p.jsonEncoder, err = structjson.NewMarshaller(p.typeOf, p.jsonOptions()...)
		if err != nil {
			return nil, err
		}
	}
	if len(p.exclude) > 0 {
		p.presentation, err = p.exclude.presentation(input.Type, "")
		if err != nil {
			return nil, err
		}
		if p.rows != nil {
			p.rowPresentation, err = p.exclude.presentation(p.rows.typeOf, p.rows.name)
			if err != nil {
				return nil, err
			}
		}
	}
	if p.custom == nil && p.jsonEncoder == nil && p.typeOf != nil {
		p.standardJSON = jsonmarshal.NewStandard(p.typeOf)
	}
	return p, nil
}

func (p *Plan) Type() reflect.Type {
	if p == nil {
		return nil
	}
	return p.typeOf
}
func (p *Plan) DefaultFormat() string {
	if p == nil || p.format == "" {
		return "json"
	}
	return p.format
}

// FormatSelector returns the single declared HTTP source for request-selected
// output formats. Without one, the legacy _format query source remains active.
func (p *Plan) FormatSelector() (spec.BindSource, bool) {
	if p == nil || p.formatSelector == nil {
		return spec.BindSource{}, false
	}
	return *p.formatSelector, true
}

func (p *Plan) JSONOnly() bool {
	if p == nil {
		return false
	}
	if p.custom != nil {
		return true
	}
	if p.typeOf == nil {
		return false
	}
	marshaller := reflect.TypeFor[json.Marshaler]()
	wire := reflect.TypeFor[JSONWireType]()
	return p.typeOf.Implements(marshaller) || reflect.PointerTo(p.typeOf).Implements(marshaller) ||
		p.typeOf.Implements(wire) || reflect.PointerTo(p.typeOf).Implements(wire)
}

func ContentType(format string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "json", "tabular":
		return "application/json", nil
	case "csv":
		return "text/csv", nil
	case "xml":
		return "application/xml", nil
	case "xls", "xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", nil
	default:
		return "", fmt.Errorf("unsupported output format %q", format)
	}
}

func (p *Plan) Encode(ctx context.Context, format string, value any) (Result, error) {
	if p == nil {
		return Result{}, fmt.Errorf("output contract is required")
	}
	if format == "" {
		format = p.DefaultFormat()
	}
	format = strings.ToLower(strings.TrimSpace(format))
	if format != "json" && p.JSONOnly() {
		return Result{}, fmt.Errorf("custom JSON output does not declare a safe %s representation", format)
	}
	contentType, err := ContentType(format)
	if err != nil {
		return Result{}, err
	}
	if value == nil {
		return Result{ContentType: contentType}, nil
	}
	actual := reflect.ValueOf(value)
	singletonRows := p.rows != nil && p.rows.name == "" && p.rows.sourceType.Kind() == reflect.Struct && (format == "csv" || format == "tabular")
	if actual.Kind() == reflect.Pointer && actual.IsNil() && format != "json" && !singletonRows {
		var data []byte
		if format == "tabular" {
			data = []byte("null")
		}
		return Result{Data: data, ContentType: contentType}, nil
	}
	if selection := dexec.SelectedOutputFields(ctx, value); format != "json" && selection != nil {
		selected, projected, err := p.selectedPresentation(value, selection)
		if err != nil {
			return Result{}, err
		}
		p, value = selected, projected
	}
	var data []byte
	switch format {
	case "json":
		data, err = p.marshalJSON(ctx, value, true)
	case "csv":
		data, err = p.marshalCSV(value)
	case "tabular":
		data, err = p.marshalTabular(ctx, value)
	case "xml":
		data, err = p.marshalXML(value)
	case "xls", "xlsx":
		data, err = p.marshalXLS(value)
	}
	filename := p.filename(format)
	disposition := p.disposition(format)
	return Result{Data: data, ContentType: contentType, Filename: filename, ContentDisposition: disposition}, err
}

func (p *Plan) filename(format string) string {
	if p.title == "" {
		return ""
	}
	switch format {
	case "csv", "xml", "xlsx":
		return p.title + "." + format
	case "xls":
		return p.title + ".xlsx"
	}
	return ""
}

func (p *Plan) disposition(format string) string {
	if name := p.filename(format); name != "" {
		return mime.FormatMediaType("attachment", map[string]string{"filename": name})
	}
	return ""
}

func (p *Plan) transformedJSON() bool {
	return p.caseFormat != "" || p.timeLayout != "" || len(p.exclude) > 0 || p.omitEmpty
}

func (p *Plan) marshalJSON(ctx context.Context, value any, custom bool) ([]byte, error) {
	if custom && p.custom != nil {
		return p.custom.Marshal(value)
	}
	selection := dexec.SelectedOutputFields(ctx, value)
	if !p.transformedJSON() {
		if selection != nil {
			return structjson.MarshalStandard(value, structjson.WithPathFieldExcluder(selection))
		}
		return json.Marshal(value)
	}
	if selection == nil && p.jsonEncoder != nil {
		return p.jsonEncoder.Marshal(value)
	}
	options := p.jsonOptions()
	if selection != nil {
		if native, ok := selection.(*structjson.FieldFilter); ok {
			formatted, err := native.WithOptions(options...)
			if err != nil {
				return nil, err
			}
			selection = formatted
		}
		options = append(options, structjson.WithPathFieldExcluder(selection))
	}
	return structjson.MarshalContext(ctx, value, options...)
}

func (p *Plan) jsonOptions() []structjson.Option {
	var options []structjson.Option
	if len(p.exclude) > 0 {
		options = append(options, structjson.WithExcludedFields(p.exclude...))
	}
	if p.omitEmpty {
		options = append(options, structjson.WithOmitEmpty(true))
	}
	if p.caseFormat != "" {
		options = append(options, structjson.WithCaseFormat(p.caseFormat))
	}
	if p.timeLayout != "" {
		options = append(options, structjson.WithFormatTag(&format.Tag{TimeLayout: p.timeLayout}))
	}
	return options
}

type rowsPlan struct {
	sourceType reflect.Type
	index      []int
	typeOf     reflect.Type
	name       string
	envelope   reflect.Type
	fields     []xshape.Field
}

func compileRows(input CompileInput) (*rowsPlan, error) {
	typeOf := (xshape.Runtime{}).Indirect(input.Type)
	if typeOf == nil {
		return nil, nil
	}
	if typeOf.Kind() == reflect.Slice || typeOf.Kind() == reflect.Array {
		return newRowsPlan(typeOf), nil
	}
	if typeOf.Kind() != reflect.Struct {
		return nil, nil
	}
	name := input.DataField
	if name == "" && input.Component != nil {
		for _, param := range input.Component.Parameters {
			if param != nil && param.Source.Kind == "output" && (param.Source.Name == "view" || param.Source.Name == "body") {
				name = param.Name
				break
			}
		}
	}
	// A declared direct singleton owns the whole row, including its relations.
	// Only an explicit holder may select a field from that row.
	if name == "" && input.Component != nil && input.Component.RootView != nil && input.Component.RootView.Cardinality == spec.CardinalityOne {
		return newRowsPlan(typeOf), nil
	}
	fields, err := xshape.Linked(typeOf).Fields()
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		if !field.Exported || len(field.Index) != 1 {
			continue
		}
		candidate := (xshape.Runtime{}).Indirect(field.ReflectedType)
		if candidate == nil || candidate.Kind() != reflect.Slice && candidate.Kind() != reflect.Array && !(name != "" && candidate.Kind() == reflect.Struct) {
			continue
		}
		if name != "" && !strings.EqualFold(name, field.Name) {
			continue
		}
		rowPlan := newRowsPlan(candidate)
		rowPlan.index, rowPlan.name, rowPlan.fields = field.Index, field.Name, fields
		var envelope []xshape.RuntimeField
		for _, item := range fields {
			if !item.Exported || len(item.Index) != 1 {
				continue
			}
			typ := item.ReflectedType
			if item.Name == field.Name {
				typ = reflect.TypeOf(json.RawMessage{})
			}
			envelope = append(envelope, xshape.RuntimeField{Name: item.Name, Type: typ, Tag: item.Tag, Anonymous: item.Anonymous})
		}
		rowPlan.envelope, err = (xshape.Runtime{}).Struct(envelope)
		return rowPlan, err
	}
	return nil, nil
}

// newRowsPlan adapts only the root carrier. Native codecs retain ownership of
// field traversal and relation encoding; structural output types are unchanged.
func newRowsPlan(source reflect.Type) *rowsPlan {
	r := &rowsPlan{sourceType: source, typeOf: source}
	switch source.Kind() {
	case reflect.Struct:
		r.typeOf = reflect.SliceOf(source)
	case reflect.Array:
		r.typeOf = reflect.SliceOf(source.Elem())
	}
	return r
}

func (r *rowsPlan) value(output any) (reflect.Value, error) {
	value := reflect.ValueOf(output)
	for value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return reflect.Zero(r.typeOf), nil
		}
		value = value.Elem()
	}
	if len(r.index) > 0 {
		if value.Kind() != reflect.Struct {
			return reflect.Value{}, fmt.Errorf("output rows require struct envelope")
		}
		value = value.FieldByIndex(r.index)
	}
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return reflect.Zero(r.typeOf), nil
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return reflect.Zero(r.typeOf), nil
	}
	if value.Type() != r.sourceType {
		return reflect.Value{}, fmt.Errorf("output rows type %v does not match %v", value.Type(), r.sourceType)
	}
	switch value.Kind() {
	case reflect.Struct:
		rows := reflect.MakeSlice(r.typeOf, 1, 1)
		rows.Index(0).Set(value)
		return rows, nil
	case reflect.Array:
		rows := reflect.MakeSlice(r.typeOf, value.Len(), value.Len())
		reflect.Copy(rows, value)
		return rows, nil
	}
	return value, nil
}
