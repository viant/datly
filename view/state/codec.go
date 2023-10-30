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
	Name         string   `json:",omitempty" yaml:"Name,omitempty" `
	Body         string   `json:",omitempty" yaml:"Body,omitempty"`
	Args         []string `json:",omitempty" yaml:"Args,omitempty"`
	Schema       *Schema  `json:",omitempty" yaml:"Schema,omitempty"`
	OutputType   string   `json:",omitempty" yaml:"OutputType,omitempty"`
	_initialized bool
	_codec       codec.Instance
}

func (v *Codec) Init(resource Resource, inputType reflect.Type) error {
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

func (v *Codec) initFnIfNeeded(resource Resource, inputType reflect.Type) error {
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
	if v.Schema == nil {
		v.Schema = &Schema{}
	}
	v.Schema.SetType(resultType)
	return nil
}

func (v *Codec) inheritCodecIfNeeded(resource Resource, inputType reflect.Type) error {
	if v.Ref == "" {
		return nil
	}

	if err := v.initSchemaIfNeeded(resource); err != nil {
		return err
	}

	aCodec, err := resource.Codecs().Lookup(v.Ref)
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
	if v.Schema == nil {
		v.Schema = &Schema{}
	}
	v.Schema.SetType(codecType)
	return nil
}

func (v *Codec) newCodecInstance(resource Resource, inputType reflect.Type, factory codec.Factory) (codec.Instance, error) {
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

func (v *Codec) extractCodecFn(resource Resource, inputType reflect.Type) (codec.Instance, error) {
	foundCodec, err := resource.Codecs().Lookup(v.Name)
	if err != nil {
		return nil, err
	}

	return v.codecInstance(resource, inputType, foundCodec)
}

func (v *Codec) codecInstance(resource Resource, inputType reflect.Type, foundCodec *codec.Codec) (codec.Instance, error) {
	if foundCodec.Factory != nil {
		return v.newCodecInstance(resource, inputType, foundCodec.Factory)
	}

	return foundCodec.Instance, nil
}

func (v *Codec) Transform(ctx context.Context, value interface{}, options ...codec.Option) (interface{}, error) {
	return v._codec.Value(ctx, value, options...)
}

func (v *Codec) initSchemaIfNeeded(resource Resource) error {
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
