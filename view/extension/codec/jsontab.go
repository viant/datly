package codec

import (
	"context"
	"github.com/viant/datly/view/extension/codec/jsontab"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/handler/response/tabular/tjson"
	"reflect"
)

const (
	KeyJsonTab = "JsonTab"
)

type (
	JsonTabFactory struct{}

	JsonTab struct {
		service *jsontab.Service
	}
)

func (e *JsonTabFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	ret := &JsonTab{service: jsontab.New()}
	return ret, nil
}

func (e *JsonTab) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&tjson.Tabular{}), nil
}

func (e *JsonTab) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)
	return e.service.Transfer(raw)
}
