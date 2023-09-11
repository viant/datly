package codec

import (
	"context"
	"github.com/viant/datly/config/codec/jsontab"
	"github.com/viant/xdatly/codec"
	"reflect"
)

const (
	KeyXmlTab = "XmlTab"
)

type (
	XmlTabFactory struct{}

	XmlTab struct {
		service *jsontab.Service
	}
)

func (e *XmlTabFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	ret := &XmlTab{service: jsontab.New()}
	return ret, nil
}

func (e *XmlTab) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&jsontab.Result{}), nil
}

func (e *XmlTab) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)
	return e.service.Transfer(raw)
}
