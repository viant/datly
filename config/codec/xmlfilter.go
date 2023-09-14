package codec

import (
	"context"
	"github.com/viant/datly/config/codec/jsontab"
	"github.com/viant/xdatly/codec"
	"reflect"
)

const (
	KeyXmlFilter = "XmlFilter"
)

type (
	XmlFilterFactory struct{}

	XmlFilter struct {
		service *jsontab.Service
	}
)

func (e *XmlFilterFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	ret := &XmlFilter{service: jsontab.New()}
	return ret, nil
}

func (e *XmlFilter) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&jsontab.Result{}), nil
}

func (e *XmlFilter) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)
	return e.service.Transfer(raw)
}
