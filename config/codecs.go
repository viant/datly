package config

import (
	"context"
	"fmt"
	"reflect"
)

type (
	CodecFn func(context context.Context, rawValue interface{}, options ...interface{}) (interface{}, error)
	Codec   struct {
		name       string
		visitor    Valuer
		resultType reflect.Type
	}

	CodecsRegistry map[string]interface{}
)

func (c *Codec) Name() string {
	return c.name
}

func (c *Codec) Valuer() Valuer {
	return c.visitor
}

func (c *Codec) ResultType(_ reflect.Type) (reflect.Type, error) {
	return c.resultType, nil
}

func NewCodec(name string, valuer Valuer, resultType reflect.Type) CodecDef {
	return &Codec{
		name:       name,
		visitor:    valuer,
		resultType: resultType,
	}
}

func UnexpectedUseError(methodName string, on interface{}) error {
	return fmt.Errorf("unexpected use %v on %T", methodName, on)
}

func NewCodecs(codecs ...Namer) CodecsRegistry {
	result := CodecsRegistry{}
	for i := range codecs {
		result.Register(codecs[i])
	}

	return result
}

func (v CodecsRegistry) Lookup(name string) (interface{}, error) {
	visitor, ok := v[name]
	if !ok {
		return nil, fmt.Errorf("not found codec with name %v", name)
	}

	return visitor, nil
}

func (v CodecsRegistry) LookupCodec(name string) (BasicCodec, error) {
	lookup, err := v.Lookup(name)
	if err != nil {
		return nil, err
	}

	aCodec, ok := lookup.(BasicCodec)
	if !ok {
		return nil, fmt.Errorf("expected %T to be type of BasicCodec", aCodec)
	}

	return aCodec, nil
}

func (v CodecsRegistry) Register(visitor Namer) {
	v[visitor.Name()] = visitor
}

func (v CodecsRegistry) RegisterWithName(name string, codec interface{}) {
	v[name] = codec
}
