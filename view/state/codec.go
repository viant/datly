package state

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/xdatly/codec"
	"reflect"
)

type Codec struct {
	shared.Reference
	Name         string   `json:",omitempty"`
	Body         string   `json:",omitempty"`
	Args         []string `json:",omitempty"`
	Schema       *Schema  `json:",omitempty"`
	OutputType   string   `json:",omitempty"`
	_initialized bool
	_codec       codec.Instance
}

func (v *Codec) Init(resource Resourcelet, inputType reflect.Type) error {
	if v._initialized {
		return nil
	}

	v._initialized = true

	if err := v.inheritCodecIfNeeded(resource, inputType); err != nil {
		return err
	}

	v.ensureSchema(inputType)

	if err := v.Schema.Init(resource); err != nil {
		return err
	}

	return v.initFnIfNeeded(resource, inputType)
}

func (v *Codec) initFnIfNeeded(resource Resourcelet, inputType reflect.Type) error {
	if v._codec != nil {
		return nil
	}

	fn, err := v.extractCodecFn(resource, inputType)
	if err != nil {
		return err
	}

	v._codec = fn
	resultType, err := fn.ResultType(inputType)
	if err != nil {
		return err
	}

	v.Schema = NewSchema(resultType)
	return nil
}

func (v *Codec) inheritCodecIfNeeded(resource Resourcelet, inputType reflect.Type) error {
	if v.Ref == "" {
		return nil
	}

	if err := v.initSchemaIfNeeded(resource); err != nil {
		return err
	}

	aCodec, err := resource.NamedCodecs().Lookup(v.Ref)
	if err != nil {
		return fmt.Errorf("not found codec with name %v", v.Ref)
	}

	instance, err := v.codecInstance(resource, inputType, aCodec)
	if err != nil {
		return err
	}

	codecType, err := instance.ResultType(inputType)
	if err != nil {
		return err
	}

	v._codec = instance
	v.Schema = NewSchema(codecType)
	return nil
}

func (v *Codec) newCodecInstance(resource Resourcelet, inputType reflect.Type, factory codec.Factory) (codec.Instance, error) {
	var opts []interface{}
	if codecOptions := resource.CodecOptions(); codecOptions != nil {
		opts = codecOptions.Options
	}
	opts = append(opts, resource.LookupType())
	var options []codec.Option
	options = append(options, codec.WithOptions(opts...))
	aCodec, err := factory.New(&codec.Config{
		Body:       v.Body,
		InputType:  inputType,
		Args:       v.Args,
		OutputType: v.OutputType,
	}, options...)

	if err != nil {
		return nil, err
	}

	return aCodec, nil
}

func (v *Codec) ensureSchema(paramType reflect.Type) {
	if v.Schema == nil {
		v.Schema = &Schema{}
		v.Schema.SetType(paramType)
	}
}

func (v *Codec) extractCodecFn(resource Resourcelet, inputType reflect.Type) (codec.Instance, error) {
	foundCodec, err := resource.NamedCodecs().Lookup(v.Name)
	if err != nil {
		return nil, err
	}

	return v.codecInstance(resource, inputType, foundCodec)
}

func (v *Codec) codecInstance(resource Resourcelet, inputType reflect.Type, foundCodec *codec.Codec) (codec.Instance, error) {
	if foundCodec.Factory != nil {
		return v.newCodecInstance(resource, inputType, foundCodec.Factory)
	}

	return foundCodec.Instance, nil
}

func (v *Codec) Transform(ctx context.Context, value interface{}, options ...codec.Option) (interface{}, error) {
	return v._codec.Value(ctx, value, options...)
}

func (v *Codec) initSchemaIfNeeded(resource Resourcelet) error {
	if v.Schema == nil || v.Schema.Type() != nil {
		return nil
	}
	return v.Schema.InitType(resource.LookupType(), false)
}

func NewCodec(name string, schema *Schema, instance codec.Instance) *Codec {
	return &Codec{
		Name:   name,
		Schema: schema,
		_codec: instance,
	}
}

// AsCodecOptions creates codec options
func AsCodecOptions(options []interface{}) []codec.Option {
	var codecOptions []codec.Option
	codecOptions = append(codecOptions, codec.WithOptions(options...))
	return codecOptions
}