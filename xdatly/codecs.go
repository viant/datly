package xdatly

import (
	"fmt"
	"reflect"
)

type (
	Codec struct {
		name       string
		visitor    Valuer
		resultType reflect.Type
	}

	CodecConfig struct {
		Query     string `json:",omitempty"`
		SourceURL string `json:",omitempty"`
		Source    string `json:",omitempty"`
	}

	CodecsRegistry map[string]BasicCodec
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

func unexpectedUseError(on interface{}) error {
	return fmt.Errorf("unexpected use Value on %T", on)
}

func NewCodecs(codecs ...BasicCodec) CodecsRegistry {
	result := CodecsRegistry{}
	for i := range codecs {
		result.Register(codecs[i])
	}

	return result
}

func (v CodecsRegistry) Lookup(name string) (BasicCodec, error) {
	visitor, ok := v[name]
	if !ok {
		return nil, fmt.Errorf("not found visitor with name %v", name)
	}

	return visitor, nil
}

func (v CodecsRegistry) Register(visitor BasicCodec) {
	v[visitor.Name()] = visitor
}
