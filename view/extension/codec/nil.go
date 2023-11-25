package codec

import (
	"context"
	"github.com/viant/xdatly/codec"
	"reflect"
)

const (
	KeyNil = "Nil"
)

type Nil struct {
}

func (i *Nil) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return paramType, nil
}

func (i *Nil) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	return nil, nil
}
