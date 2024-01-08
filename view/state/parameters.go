package state

import (
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state/predicate"
	"github.com/viant/datly/view/tags"
	"github.com/viant/structology"
	stags "github.com/viant/tagly/tags"
	"github.com/viant/toolbox"
	"github.com/viant/velty"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strings"
)

const (
	SetMarkerTag      = `setMarker:"true" format:"-" sqlx:"-" diff:"-" `
	TypedSetMarkerTag = SetMarkerTag + ` typeName:"%s"`
)

type (

	// NamedParameters represents Parameter map indexed by Parameter.Name
	NamedParameters map[string]*Parameter

	// Parameters represents slice of parameters
	Parameters []*Parameter
)

func (s NamedParameters) LookupByLocation(kind Kind, location string) *Parameter {
	for _, candidate := range s {
		if ret := candidate.matchByLocation(kind, location); ret != nil {
			return ret
		}
	}
	return nil
}

// LookupByLocation returns match parameter by location
func (p Parameters) LookupByLocation(kind Kind, location string) *Parameter {
	if len(p) == 0 {
		return nil
	}
	for _, candidate := range p {
		if ret := candidate.matchByLocation(kind, location); ret != nil {
			return ret
		}
	}
	return nil
}

func (p Parameters) UsedBy(text string) Parameters {
	var result = Parameters{}
	for _, candidate := range p {
		if candidate.IsUsedBy(text) {
			result = append(result, candidate)
		}
	}
	return result
}
func (p Parameters) External() Parameters {
	var result Parameters
	for i, parameter := range p {
		switch parameter.In.Kind {
		case KindHeader, KindQuery, KindRequestBody, KindCookie, KindRequest:
			result = append(result, p[i])
		}
	}
	return result
}

func (p *Parameter) matchByLocation(kind Kind, location string) *Parameter {
	switch p.In.Kind {
	case kind:
		if p.In.Name == location {
			return p
		}
	case KindRepeated:
		for _, parameter := range p.Repeated {
			if parameter.In.Name == location {
				return parameter
			}
		}
	case KindObject:
		for _, parameter := range p.Object {
			if parameter.In.Name == location {
				return parameter
			}
		}
	}
	return nil
}

func (p Parameters) FilterByKind(kind Kind) Parameters {
	var result = Parameters{}
	for _, candidate := range p {
		candidate.matchByKind(kind, &result)
	}
	return result
}

func (p *Parameter) matchByKind(kind Kind, result *Parameters) {
	if p.In == nil {
		panic(fmt.Sprintf("kind was nil %s\n", p.Name))
	}
	switch p.In.Kind {
	case kind:
		*result = append(*result, p)
	case KindObject:
		for _, parameter := range p.Object {
			if parameter.In.Kind == kind {
				*result = append(*result, parameter)
				continue
			}
			switch parameter.In.Kind {
			case KindRepeated:
				if values := Parameters(parameter.Repeated).FilterByKind(kind); len(values) > 0 {
					*result = append(*result, values...)
				}
			case KindObject:
				if values := Parameters(parameter.Object).FilterByKind(kind); len(values) > 0 {
					*result = append(*result, values...)
				}
			}
		}
	case KindRepeated:
		for _, parameter := range p.Repeated {
			if parameter.In.Kind == kind {
				*result = append(*result, parameter)
				continue
			}
			switch parameter.In.Kind {
			case KindRepeated:
				if values := Parameters(parameter.Repeated).FilterByKind(kind); len(values) > 0 {
					*result = append(*result, values...)
				}
			case KindObject:
				if values := Parameters(parameter.Object).FilterByKind(kind); len(values) > 0 {
					*result = append(*result, values...)
				}
			}

		}
	}
}

func (p Parameters) GroupByStatusCode() []Parameters {
	var result []Parameters
	var unAuthorizedParameters Parameters
	var forbiddenParameters Parameters
	var external Parameters
	var transient Parameters
	var others Parameters

	for i, candidate := range p {
		switch candidate.ErrorStatusCode {
		case http.StatusUnauthorized, http.StatusProxyAuthRequired:
			unAuthorizedParameters = append(unAuthorizedParameters, p[i])
		case http.StatusForbidden, http.StatusNotAcceptable, http.StatusMethodNotAllowed:
			forbiddenParameters = append(forbiddenParameters, p[i])
		default:
			switch candidate.In.Kind {
			case KindHeader, KindConst, KindLiteral, KindQuery, KindRequestBody, KindCookie, KindRequest:
				external = append(external, p[i])
			case KindParam, KindState:
				transient = append(transient, p[i])
			default:
				others = append(others, p[i])

			}
		}
	}
	if len(unAuthorizedParameters) > 0 {
		result = append(result, unAuthorizedParameters)
	}
	if len(forbiddenParameters) > 0 {
		result = append(result, forbiddenParameters)
	}
	if len(external) > 0 {
		result = append(result, external)
	}
	if len(transient) > 0 {
		result = append(result, transient)
	}

	if len(others) > 0 {
		result = append(result, others)
	}
	return result
}

func (p Parameters) SetLiterals(state *structology.State) (err error) {
	for _, parameter := range p.FilterByKind(KindConst) {
		if parameter._selector == nil {
			parameter._selector = state.Type().Lookup(parameter.Name)
		}
		if parameter.Value == nil {
			switch parameter.Schema.rType.Kind() {
			case reflect.String:
				parameter.Value = ""
			case reflect.Int:
				parameter.Value = 0
			case reflect.Float64:
				parameter.Value = 0.0
			case reflect.Bool:
				parameter.Value = false

			}
		}
		if err = parameter._selector.SetValue(state.Pointer(), parameter.Value); err != nil {
			return err
		}
	}
	return nil
}

func (p Parameters) InitRepeated(state *structology.State) (err error) {
	for _, parameter := range p {
		parameterType := parameter.OutputType()
		if parameterType == nil || parameterType.Kind() != reflect.Slice {
			continue
		}
		aSlice := reflect.MakeSlice(parameter.OutputType(), 1, 1).Interface()
		if err = state.SetValue(parameter.Name, aSlice); err != nil {
			return err
		}
	}
	return nil
}

var boolType = reflect.TypeOf(true)

type (
	reflectOptions struct {
		withSetterMarker bool
		typeName         string
		locationInput    Parameters
		withRelation     bool
		withSQL          bool
		withVelty        *bool
	}
	ReflectOption func(o *reflectOptions)
)

func newReflectOptions(opts []ReflectOption) *reflectOptions {
	ret := &reflectOptions{}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}
func WithSetMarker() ReflectOption {
	return func(o *reflectOptions) {
		o.withSetterMarker = true
	}
}

func WithVelty(flag bool) ReflectOption {
	return func(o *reflectOptions) {
		o.withVelty = &flag
	}
}

func WithSQL() ReflectOption {
	return func(o *reflectOptions) {
		o.withSQL = true
	}
}

func WithRelation() ReflectOption {
	return func(o *reflectOptions) {
		o.withRelation = true
	}
}
func WithTypeName(name string) ReflectOption {
	return func(o *reflectOptions) {
		o.typeName = name
	}
}

// LocationInput returns location input
func (p Parameters) LocationInput() Parameters {
	var result = Parameters{}
	var used = map[string]bool{}
	p.buildLocationParameters(used, &result)
	return result
}

func (p Parameters) buildLocationParameters(used map[string]bool, result *Parameters) {
	for _, param := range p {
		if len(param.Object) > 0 {
			param.Object.buildLocationParameters(used, result)
		}
		if len(param.Repeated) > 0 {
			param.Repeated.buildLocationParameters(used, result)
		}
		if input := param.LocationInput; input != nil {
			for _, item := range input.Parameters {
				if used[item.Name] {
					continue
				}
				used[item.Name] = true
				*result = append(*result, item)

			}
		}
	}
}

func (p Parameters) ReflectType(pkgPath string, lookupType xreflect.LookupType, opts ...ReflectOption) (reflect.Type, error) {
	var fields []reflect.StructField
	var setMarkerFields []reflect.StructField
	//TODO add compaction here
	var used = map[string]bool{}
	for _, param := range p {
		if used[param.Name] {
			continue
		}
		used[param.Name] = true
		structField, markerField, err := param.buildField(pkgPath, lookupType)
		if err != nil {
			return nil, err
		}
		fields = append(fields, structField)
		setMarkerFields = append(setMarkerFields, markerField)
	}

	options := newReflectOptions(opts)
	if input := options.locationInput; len(input) > 0 {
		for _, param := range input {
			if used[param.Name] {
				continue
			}
			used[param.Name] = true
			structField, markerField, err := param.buildField(pkgPath, lookupType)
			if err != nil {
				return nil, err
			}
			fields = append(fields, structField)
			setMarkerFields = append(setMarkerFields, markerField)
		}
	}

	if options.withSetterMarker && len(fields) > 0 {
		setMarkerType := reflect.StructOf(setMarkerFields)
		fields = append(fields, reflect.StructField{Name: "Has", Type: reflect.PtrTo(setMarkerType), PkgPath: xreflect.PkgPath("Has", pkgPath), Tag: reflect.StructTag(fmt.Sprintf(TypedSetMarkerTag, options.typeName+"Has"))})
	}
	if len(fields) == 0 {
		return emptyStruct, nil
	}
	baseType := reflect.StructOf(fields)
	return baseType, nil
}

func (p *Parameter) buildField(pkgPath string, lookupType xreflect.LookupType) (structField reflect.StructField, markerField reflect.StructField, err error) {
	schema := p.OutputSchema()
	if schema == nil {
		return structField, markerField, fmt.Errorf("invalid parameter: %v schema was empty", p.Name)
	}
	rType := schema.Type()
	dt := schema.DataType
	if dt == "" {
		dt = schema.Name
	}
	if rType == nil {
		rType, err = types.LookupType(lookupType, schema.DataType, xreflect.WithPackage(schema.Package))
		if err != nil {
			rType, err = types.LookupType(lookupType, schema.DataType, xreflect.WithPackage(pkgPath))
			if err != nil {
				return structField, markerField, fmt.Errorf("failed to detect parmater '%v' type for: %v  %w", p.Name, schema.TypeName(), err)
			}
		}
	}
	fieldName := p.Name
	p.Schema.Cardinality = schema.Cardinality
	if p.Schema.Cardinality == Many && (rType.Kind() != reflect.Slice && rType.Kind() != reflect.Map) {
		rType = reflect.SliceOf(rType)
	}
	if rType != nil {
		if index := strings.LastIndex(fieldName, "."); index != -1 {
			fieldName = fieldName[index+1:]
		}

		structField = reflect.StructField{Name: fieldName,
			Type:    rType,
			PkgPath: xreflect.PkgPath(fieldName, pkgPath),
			Tag:     p.buildTag(fieldName),
		}

		if fieldName == rType.Name() && strings.Contains(p.Tag, "anonymous") {
			structField.Anonymous = true
		}
	}
	markerTag := buildMarkerFieldTag(structField)
	markerField = reflect.StructField{Name: fieldName, Type: boolType, Tag: reflect.StructTag(markerTag.Stringify()), PkgPath: xreflect.PkgPath(fieldName, pkgPath)}
	return structField, markerField, nil
}

func buildMarkerFieldTag(structField reflect.StructField) stags.Tags {
	markerFieldTags := stags.NewTags(string(structField.Tag))
	var updated = stags.Tags{}
	for _, item := range markerFieldTags {
		switch item.Name {
		case "velty", "json":
			updated = append(updated, item)
		}
	}
	return updated
}

func (p Parameters) BuildBodyType(pkgPath string, lookupType xreflect.LookupType) (reflect.Type, error) {
	candidates := p.FilterByKind(KindRequestBody)
	bodyLeafParameters := make(Parameters, 0, len(candidates))
	for i, candidate := range candidates {
		if candidate.In.Name != "" {
			bodyParameter := *candidates[i]
			bodyParameter.Name = candidate.In.Name
			bodyLeafParameters = append(bodyLeafParameters, &bodyParameter)
			continue
		}
		return candidate.Schema.Type(), nil
	}
	return bodyLeafParameters.ReflectType(pkgPath, lookupType, WithSetMarker())
}

// Append appends parameter
func (p *Parameters) Append(parameter *Parameter) {
	for _, param := range *p {
		if param.Name == parameter.Name {
			return
		}
	}
	*p = append(*p, parameter)
}

// Lookup returns match parameter or nil
func (p Parameters) Lookup(name string) *Parameter {
	for _, param := range p {
		if param.Name == name {
			return param
		}
	}
	return nil
}

// Index indexes parameters by Parameter.Name
func (p Parameters) Index() NamedParameters {
	result := NamedParameters(make(map[string]*Parameter))
	for i, parameter := range p {
		if _, ok := result[parameter.Name]; ok {
			continue
		}
		result[parameter.Name] = p[i]
		for _, item := range parameter.Object {
			result[item.Name] = item
		}
		for _, item := range parameter.Repeated {
			result[item.Name] = item
		}
	}
	return result
}

// Filter filters Parameters with given Kind and creates Template
func (p Parameters) Filter(kind Kind) NamedParameters {
	index := make(map[string]*Parameter)

	for parameterIndex := range p {
		if p[parameterIndex].In.Kind != kind {
			continue
		}
		index[p[parameterIndex].In.Name] = p[parameterIndex]

	}

	return index
}

func (p Parameters) PredicateStructType(d Documentation) reflect.Type {
	var fields []*predicate.FilterType
	fieldTypes := map[string]*predicate.FilterType{}
	for _, candidate := range p {
		if len(candidate.Predicates) == 0 {
			continue
		}
		aTag, _ := tags.ParseStateTags(reflect.StructTag(candidate.Tag), nil)
		pTag := aTag.EnsurePredicate()
		pTag.Init(candidate.Name)
		filterType, ok := fieldTypes[pTag.Filter]
		if !ok {
			filterType = &predicate.FilterType{ParameterType: candidate.OutputType(), Tag: pTag}
			fieldTypes[pTag.Filter] = filterType

			fields = append(fields, filterType)
		}
		if pTag.Exclusion {
			filterType.ExcludeTag = candidate.Tag
		} else {
			filterType.IncludeTag = candidate.Tag
		}
	}

	var structFields []reflect.StructField
	for _, field := range fields {
		fieldTags := stags.NewTags(field.StructTagTag())
		fieldTags.SetIfNotFound("json", ",omitempty")

		if d != nil {
			fieldDescription, ok := d.ByName(field.Tag.Filter)
			if ok {
				fieldTags.Set(tags.DocumentationTag, fieldDescription)
			}
		}

		structFields = append(structFields, reflect.StructField{
			Name: field.Tag.Filter,
			Type: field.Type(),
			Tag:  reflect.StructTag(fieldTags.Stringify()),
		})
	}
	if len(structFields) == 0 {
		return emptyStruct
	}
	return reflect.StructOf(structFields)
}

func (p *Parameter) buildTag(fieldName string) reflect.StructTag {
	aTag := tags.Tag{}
	name := p.Name
	if fieldName == p.Name {
		name = ""
	}
	aTag.Parameter = &tags.Parameter{
		Name:  name,
		Kind:  string(p.In.Kind),
		In:    string(p.In.Name),
		When:  p.When,
		Scope: p.Scope,
		With:  p.With,
	}
	if p.Output != nil && p.Output.Schema != nil {
		if p.Output.Schema.TypeName() != p.Schema.TypeName() {
			aTag.Parameter.DataType = p.Schema.TypeName()
		}
	}
	if p.Handler != nil {
		aTag.Handler = &tags.Handler{Name: p.Handler.Name, Arguments: p.Handler.Args}
	}
	if strings.Contains(aTag.Parameter.In, ",") {
		aTag.Parameter.In = "{" + aTag.Parameter.In + "}"
	}
	setter.SetStringIfEmpty(&aTag.Documentation, p.Description)
	if p.Value != nil {
		val := toolbox.AsString(p.Value)
		aTag.Value = &val
	}
	if p.Output != nil {
		aTag.Codec = &tags.Codec{Name: p.Output.Name, Arguments: p.Output.Args}
	}
	if p.Predicates != nil {
		for _, aPredicate := range p.Predicates {
			aTag.Predicates = append(aTag.Predicates, &tags.Predicate{Name: aPredicate.Name, Group: aPredicate.Group, Arguments: aPredicate.Args})
		}
	}

	switch p.In.Kind {
	case KindObject:
		aTag.TypeName = SanitizeTypeName(p.Name)
	}
	result := aTag.UpdateTag(reflect.StructTag(p.Tag))
	return result
}

func (p NamedParameters) Merge(with NamedParameters) {
	for s := range with {
		p[s] = with[s]
	}
}

// Lookup returns Parameter with given name
func (p NamedParameters) Lookup(name string) (*Parameter, error) {
	if param, ok := p[name]; ok {
		return param, nil
	}
	return nil, fmt.Errorf("not found parameter %v", name)
}

// Register registers parameter
func (p NamedParameters) Register(parameter *Parameter) error {
	if _, ok := p[parameter.Name]; ok {
		fmt.Printf("[WARN] parameter with %v name already exists in given resource", parameter.Name)
	}

	p[parameter.Name] = parameter
	return nil
}

// WithParameterType returns schema type parameter option
func WithParameterType(t reflect.Type) ParameterOption {
	return func(p *Parameter) {
		switch t.Kind() {
		case reflect.String, reflect.Int, reflect.Float64, reflect.Float32, reflect.Bool:
			p.Schema = &Schema{DataType: t.Kind().String()}
			return
		}
		p.Schema = NewSchema(t)
	}
}

func WithParameterTag(tag string) ParameterOption {
	return func(p *Parameter) {
		p.Tag = tag
	}
}

// NewRefParameter creates a new ref parameter
func NewRefParameter(name string) *Parameter {
	return &Parameter{Reference: shared.Reference{Ref: name}}
}

// NewParameter creates a parameter
func NewParameter(name string, in *Location, opts ...ParameterOption) *Parameter {
	ret := &Parameter{Name: name, In: in}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func PkgPath(fieldName string, pkgPath string) (fieldPath string) {

	if fieldName[0] > 'Z' || fieldName[0] < 'A' {
		fieldPath = pkgPath
	}
	return fieldPath
}

func fieldByTemplateName(structType reflect.Type, name string) (*xunsafe.Field, error) {
	structType = shared.Elem(structType)

	field, ok := structType.FieldByName(name)
	if !ok {
		for i := 0; i < structType.NumField(); i++ {
			field = structType.Field(i)
			veltyTag := velty.Parse(field.Tag.Get("velty"))
			for _, fieldName := range veltyTag.Names {
				if fieldName == name {
					return xunsafe.NewField(field), nil
				}
			}
		}

		return nil, fmt.Errorf("not found field %v at type %v", name, structType.String())
	}

	return xunsafe.NewField(field), nil
}

func WithParameterSchema(schema *Schema) ParameterOption {
	return func(p *Parameter) {
		p.Schema = schema
	}
}

func WithCachable(flag bool) ParameterOption {
	return func(p *Parameter) {
		p.Cacheable = &flag
	}
}

func WithLocationInput(parameters Parameters) ReflectOption {
	return func(o *reflectOptions) {
		o.locationInput = parameters
	}
}
