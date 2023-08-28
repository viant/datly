package state

import (
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state/predicate"
	"github.com/viant/structology"
	"github.com/viant/velty"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strings"
)

type (

	// NamedParameters represents Parameter map indexed by Parameter.Name
	NamedParameters map[string]*Parameter

	// Parameters represents slice of parameters
	Parameters []*Parameter
)

// LookupByLocation returns match parameter by location
func (p Parameters) LookupByLocation(kind Kind, location string) *Parameter {
	if len(p) == 0 {
		return nil
	}
	for _, candidate := range p {
		if candidate.In.Kind == kind && candidate.In.Name == location {
			return candidate
		}
	}
	return nil
}

func (p Parameters) FilterByKind(kind Kind) Parameters {
	var result = Parameters{}
	for i, candidate := range p {
		if candidate.In.Kind == kind {
			result = append(result, p[i])
		}
	}
	return result
}

func (p Parameters) GroupByStatusCode() []Parameters {
	var result []Parameters
	var unAuthorizedParameters Parameters
	var forbiddenParameters Parameters
	var others Parameters
	for i, candidate := range p {
		switch candidate.ErrorStatusCode {
		case http.StatusUnauthorized, http.StatusProxyAuthRequired:
			unAuthorizedParameters = append(unAuthorizedParameters, p[i])
		case http.StatusForbidden, http.StatusNotAcceptable, http.StatusMethodNotAllowed:
			forbiddenParameters = append(forbiddenParameters, p[i])
		default:
			others = append(others, p[i])
		}
	}
	if len(unAuthorizedParameters) > 0 {
		result = append(result, unAuthorizedParameters)
	}
	if len(forbiddenParameters) > 0 {
		result = append(result, forbiddenParameters)
	}
	if len(others) > 0 {
		result = append(result, others)
	}
	return result
}

func (s Parameters) SetLiterals(state *structology.State) (err error) {
	for _, parameter := range s.FilterByKind(KindLiteral) {
		if parameter._selector == nil {
			parameter._selector = state.Type().Lookup(parameter.Name)
		}
		if err = parameter._selector.SetValue(state.Pointer(), parameter.Const); err != nil {
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

func (s Parameters) ReflectType(pkgPath string, lookupType xreflect.LookupType, withSetMarker bool) (reflect.Type, error) {
	var fields []reflect.StructField
	var setMarkerFields []reflect.StructField

	var err error
	for _, param := range s {
		schema := param.OutputSchema()
		if schema == nil {
			return nil, fmt.Errorf("invalid parameter: %v schema was empty", param.Name)
		}
		if schema.DataType == "" && param.DataType != "" {
			schema.DataType = param.DataType
		}
		rType := schema.Type()
		if rType == nil {
			if rType, err = types.LookupType(lookupType, schema.DataType); err != nil {
				return nil, fmt.Errorf("failed to detect parmater '%v' type for: %v  %w", param.Name, schema.DataType, err)
			}
		}
		param.Schema.Cardinality = schema.Cardinality
		if rType != nil {

			structField := reflect.StructField{Name: param.Name, Type: rType, PkgPath: PkgPath(param.Name, pkgPath), Tag: reflect.StructTag(param.Tag)}
			if param.Name == rType.Name() || strings.Contains(param.Tag, "anonymous") {
				structField.Anonymous = true
			}
			fields = append(fields, structField)
			setMarkerFields = append(setMarkerFields, reflect.StructField{Name: param.Name, Type: boolType, PkgPath: PkgPath(param.Name, pkgPath), Tag: reflect.StructTag(param.Tag)})
		}
	}

	if withSetMarker && len(fields) > 0 {
		setMarkerType := reflect.StructOf(setMarkerFields)
		fields = append(fields, reflect.StructField{Name: "Has", Type: reflect.PtrTo(setMarkerType), PkgPath: PkgPath("Has", pkgPath), Tag: `setMarker:"true" sqlx:"-" diff:"-"  `})
	}
	if len(fields) == 0 {
		return reflect.TypeOf(struct{}{}), nil
	}
	baseType := reflect.StructOf(fields)
	return baseType, nil
}

//func (p Parameters) Len() int {
//	return len(p)
//}
//
//func (p Parameters) Less(i, j int) bool {
//	if p[j].ErrorStatusCode == p[i].ErrorStatusCode {
//		return p[j].IsRequired()
//	}
//
//	if p[j].ErrorStatusCode == 401 {
//		return false
//	}
//
//	if p[j].ErrorStatusCode == 403 {
//		return p[i].ErrorStatusCode == 401
//	}
//
//	return true
//}

func (p Parameters) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
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
	}
	return result
}

// Filter filters Parameters with given Kind and creates Template
func (p Parameters) Filter(kind Kind) NamedParameters {
	result := make(map[string]*Parameter)

	for parameterIndex := range p {
		if p[parameterIndex].In.Kind != kind {
			continue
		}
		result[p[parameterIndex].In.Name] = p[parameterIndex]

	}

	return result
}

func (p Parameters) PredicateStructType() reflect.Type {
	var fields []*predicate.FilterType
	fieldTypes := map[string]*predicate.FilterType{}
	for _, candidate := range p {
		if len(candidate.Predicates) == 0 {
			continue
		}
		tagText, _ := reflect.StructTag(candidate.Tag).Lookup(predicate.TagName)
		tag := predicate.ParseTag(tagText, candidate.Name)
		filterType, ok := fieldTypes[tag.Name]
		if !ok {
			filterType = &predicate.FilterType{ParameterType: candidate.OutputType(), Tag: tag}
			fieldTypes[tag.Name] = filterType
			fields = append(fields, filterType)
		}
		if tag.Exclusion {
			filterType.ExcludeTag = candidate.Tag
		} else {
			filterType.IncludeTag = candidate.Tag
		}
		if ok {
			continue
		}
	}

	var structFields []reflect.StructField
	for _, field := range fields {
		structFields = append(structFields, reflect.StructField{
			Name: field.Name,
			Type: field.Type(),
			Tag:  reflect.StructTag(`json:",omitempty" ` + strings.Trim(string(field.StructTagTag()), "`")),
		})
	}

	if len(structFields) == 0 {
		return reflect.TypeOf(struct{}{})
	}
	return reflect.StructOf(structFields)
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
