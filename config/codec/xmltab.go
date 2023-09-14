package codec

import (
	"context"
	"github.com/viant/datly/config/codec/xmltab"
	"github.com/viant/xdatly/codec"
	"reflect"
)

const (
	KeyXmlTab = "XmlTab"
)

type (
	XmlTabFactory struct{}

	XmlTab struct {
		service *xmltab.Service
	}
)

func (e *XmlTabFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	ret := &XmlTab{service: xmltab.New()}
	return ret, nil
}

func (e *XmlTab) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&xmltab.Result{}), nil
}

func (e *XmlTab) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)
	return e.service.Transfer(raw)
}
