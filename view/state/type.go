package state

import (
	"context"
	"embed"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/tags"
	"github.com/viant/structology"
	"github.com/viant/tagly/format/text"
	"reflect"
	"strings"
	"unicode"
)

type (

	//Type represents parameters/schema derived state type
	Type struct {
		*Schema
		Parameters   Parameters `json:",omitempty" yaml:"Parameters"`
		withMarker   bool
		stateType    *structology.StateType
		resource     Resource
		fs           *embed.FS
		Package      string
		withBodyType bool
		Doc          Documentation
	}
	Option func(t *Type)
)

// Type returns structorlogy state
func (t *Type) Type() *structology.StateType {
	return t.stateType
}

// IsAnonymous returns flag for basic unwrapped style
func (t *Type) IsAnonymous() bool {
	return t.AnonymousParameters() != nil
}

func (t *Type) AnonymousParameters() *Parameter {
	if len(t.Parameters) != 1 {
		return nil
	}
	if strings.Contains(t.Parameters[0].Tag, "anonymous") {
		return t.Parameters[0]
	}
	return nil
}

func (t *Type) apply(options []Option) {
	for _, opt := range options {
		opt(t)
	}
	return
}

func (t *Type) Init(options ...Option) error {
	t.apply(options)
	hasParameters := len(t.Parameters) > 0
	if !hasParameters && t.Schema == nil {
		t.Schema = EmptySchema()
	}

	if rType := t.Schema.Type(); rType != nil && !hasParameters {
		if err := t.buildParameters(); err != nil {
			return err
		}
	} else if hasParameters && t.Schema.Type() == nil {
		if err := t.buildSchema(context.Background(), t.withMarker); err != nil {
			return err
		}
	} else if t.Schema == nil {
		t.Schema = EmptySchema()
	} else {
		for _, parameter := range t.Parameters {
			if err := parameter.Init(context.Background(), t.resource); err != nil {
				return err
			}
		}
	}
	rType := t.Schema.Type()
	if rType == nil {
		return fmt.Errorf("actual type was nil")
	}
	t.SetType(rType)
	return nil
}

func (t *Type) SetType(rType reflect.Type) {
	t.Schema.SetType(rType)
	t.stateType = structology.NewStateType(rType)
}

func (t *Type) buildSchema(ctx context.Context, withMarker bool) (err error) {
	hasBodyParam := false
	for _, parameter := range t.Parameters {
		if parameter.In.Kind == KindRequestBody {
			hasBodyParam = true
		}
		if err = parameter.Init(ctx, t.resource); err != nil {
			return err
		}
	}
	if t.withBodyType && !hasBodyParam {
		t.withBodyType = hasBodyParam
	}
	var rType reflect.Type
	if t.withBodyType {
		rType, err = t.Parameters.BuildBodyType(pkgPath, t.resource.LookupType())
	} else {
		var opts []ReflectOption
		if withMarker {
			opts = append(opts, WithSetMarker())
		}
		rType, err = t.Parameters.ReflectType(pkgPath, t.resource.LookupType(), opts...)
	}
	if err != nil {
		return err
	}
	if rType.Kind() == reflect.Struct {
		rType = reflect.PtrTo(rType)
	}
	if t.Schema == nil {
		t.Schema = &Schema{}
	}
	t.Schema.SetType(rType)
	//	t.Schema = NewSchema(rType)
	return nil
}

func (t *Type) buildParameter(field reflect.StructField) (*Parameter, error) {
	return BuildParameter(&field, t.fs)
}

func BuildParameter(field *reflect.StructField, fs *embed.FS) (*Parameter, error) {
	aTag, err := tags.ParseStateTags(field.Tag, fs)
	if err != nil {
		return nil, err
	}
	pTag := aTag.Parameter
	if pTag != nil {
		setter.SetStringIfEmpty(&pTag.Name, field.Name)
	}
	value, err := aTag.GetValue(field.Type)
	if err != nil {
		return nil, fmt.Errorf("invalid parameter %v value: %w", pTag.Name, err)
	}
	tag := tags.ExcludeStateTags(string(field.Tag))
	result := &Parameter{Description: aTag.Documentation, Tag: tag, Value: value}
	result.Name = field.Name
	if pTag.Name != "" {
		result.Name = pTag.Name
	}
	result.Tag = string(field.Tag)
	result.In = &Location{Kind: Kind(pTag.Kind), Name: pTag.In}
	result.Scope = pTag.Scope
	result.When = pTag.When
	result.Lazy = pTag.Lazy
	result.With = pTag.With
	required := field.Type.Kind() == reflect.Ptr || pTag.Required
	if required {
		result.Required = &required
	}

	switch result.In.Kind {
	case KindObject:
		fieldType := field.Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		for i := 0; i < fieldType.NumField(); i++ {
			objectField := fieldType.Field(i)
			if _, ok := objectField.Tag.Lookup(tags.ParameterTag); !ok {
				continue
			}
			itemParam, err := BuildParameter(&objectField, fs)
			if err != nil {
				return nil, err
			}
			result.Object = append(result.Object, itemParam)
		}
	}
	BuildCodec(aTag, result)
	BuildSchema(field, pTag, result)
	BuildPredicate(aTag, result)
	return result, nil
}

func BuildSchema(field *reflect.StructField, pTag *tags.Parameter, result *Parameter) {

	rawType := field.Type
	if rawType.Kind() == reflect.Slice {
		rawType = rawType.Elem()
	}
	if rawType.Kind() == reflect.Ptr {
		rawType = rawType.Elem()
	}
	rawName := rawType.Name()
	if pTag.DataType != "" {
		result.Schema = &Schema{Name: pTag.DataType}
		if result.Output == nil {
			result.Schema.SetType(field.Type)
		} else if result.Output.Schema == nil {
			result.Output.Schema = &Schema{}
			result.Output.Schema.SetType(field.Type)
			setter.SetStringIfEmpty(&result.Output.Schema.DataType, rawName)
		}
	} else {
		result.Schema = NewSchema(field.Type)
	}
	setter.SetStringIfEmpty(&result.Schema.DataType, rawName)
}

func (t *Type) buildParameters() error {
	structType := types.EnsureStruct(t.Schema.Type())
	for i := 0; i < structType.NumField(); i++ {
		parameter, err := t.buildParameter(structType.Field(i))
		if err != nil {
			return err
		}
		t.Parameters = append(t.Parameters, parameter)
	}
	return nil
}

func NewType(option ...Option) (*Type, error) {
	ret := &Type{}
	ret.apply(option)
	err := ret.Init()
	return ret, err
}

func WithResource(resource Resource) Option {
	return func(t *Type) {
		t.resource = resource
	}
}

func WithDoc(doc Documentation) Option {
	return func(t *Type) {
		t.Doc = doc
	}
}

func WithSchema(schema *Schema) Option {
	return func(t *Type) {
		t.Schema = schema
	}
}

func WithParameters(parameters Parameters) Option {
	return func(t *Type) {
		t.Parameters = parameters
	}
}

func WithPackage(pkg string) Option {
	return func(t *Type) {
		t.Package = pkg
	}
}

func WithSchemaPackage(pkg string) SchemaOption {
	return func(s *Schema) {
		s.SetPackage(pkg)
	}
}

func WithModulePath(aPath string) SchemaOption {
	return func(s *Schema) {
		s.ModulePath = aPath
	}
}

func WithSchemaMethods(methods []reflect.Method) SchemaOption {
	return func(s *Schema) {
		s.Methods = methods
	}
}

func WithFS(fs *embed.FS) Option {
	return func(t *Type) {
		t.fs = fs
	}
}

func WithMarker(marker bool) Option {
	return func(t *Type) {
		t.withMarker = marker
	}
}

func WithBodyType(bodyType bool) Option {
	return func(t *Type) {
		t.withBodyType = bodyType
	}
}

func SanitizeTypeName(typeName string) string {
	var runes []rune
	for _, r := range typeName {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' {
			runes = append(runes, r)
		}
	}
	name := string(runes)
	from := text.DetectCaseFormat(name)
	if from == text.CaseFormatUpperCamel {
		return name
	}
	return from.To(text.CaseFormatUpperCamel).Format(name)
}

func RawComponentType(typeName string) string {
	if strings.HasPrefix(typeName, "[]") {
		typeName = typeName[2:]
	}
	if strings.HasPrefix(typeName, "*") {
		typeName = typeName[1:]
	}
	return typeName
}
