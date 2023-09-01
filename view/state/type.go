package state

import (
	"context"
	"embed"
	"github.com/viant/datly/utils/types"
	"github.com/viant/structology"
	"reflect"
	"strings"
)

type (

	//Type represents parameters/schema derived state type
	Type struct {
		*Schema
		Parameters Parameters

		withMarker   bool
		stateType    *structology.StateType
		resourcelet  Resourcelet
		fs           *embed.FS
		Pkg          string
		withBodyType bool
	}
	Option func(t *Type)
)

// Type returns structorlogy state
func (t *Type) Type() *structology.StateType {
	return t.stateType
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
	} else if hasParameters {
		if err := t.buildSchema(context.Background(), t.withMarker); err != nil {
			return err
		}
	} else if t.Schema == nil {
		t.Schema = EmptySchema()
	}
	rType := t.Schema.Type()
	t.stateType = structology.NewStateType(rType)
	return nil
}

func (t *Type) buildSchema(ctx context.Context, withMarker bool) (err error) {
	hasBodyParam := false
	for _, parameter := range t.Parameters {
		if parameter.In.Kind == KindRequestBody {
			hasBodyParam = true
		}
		if err = parameter.Init(ctx, t.resourcelet); err != nil {
			return err
		}
	}
	if t.withBodyType && !hasBodyParam {
		t.withBodyType = hasBodyParam
	}
	var rType reflect.Type
	if t.withBodyType {
		rType, err = t.Parameters.BuildBodyType(pkgPath, t.resourcelet.LookupType())
	} else {
		rType, err = t.Parameters.ReflectType(pkgPath, t.resourcelet.LookupType(), withMarker)
	}
	if err != nil {
		return err
	}
	if rType.Kind() == reflect.Struct {
		rType = reflect.PtrTo(rType)
	}
	t.Schema = NewSchema(rType)
	return nil
}

func (t *Type) buildParameter(field reflect.StructField) (*Parameter, error) {
	result := &Parameter{}
	paramTag, err := ParseTag(TagName, t.fs)
	if err != nil {
		return nil, err
	}
	result.Name = field.Name
	result.In = &Location{Kind: Kind(paramTag.Kind), Name: paramTag.In}
	result.Schema = NewSchema(field.Type)
	BuildPredicate(field.Tag, result)
	return result, nil
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

func WithResourcelet(resourcelet Resourcelet) Option {
	return func(t *Type) {
		t.resourcelet = resourcelet
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
		t.Pkg = pkg
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
