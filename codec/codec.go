package codec

import (
	"reflect"
)

type (
	Codec interface {
		LifecycleVisitor
		ResultType() reflect.Type
	}

	codec struct {
		name       string
		visitor    Valuer
		resultType reflect.Type
	}
)

func (c *codec) Name() string {
	return c.name
}

func (c *codec) Valuer() Valuer {
	return c.visitor
}

func (c *codec) ResultType() reflect.Type {
	return c.resultType
}

func NewCodec(name string, valuer Valuer, resultType reflect.Type) Codec {
	return &codec{
		name:       name,
		visitor:    valuer,
		resultType: resultType,
	}
}
