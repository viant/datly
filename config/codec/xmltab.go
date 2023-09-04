package codec

import (
	"context"
	"github.com/viant/datly/config/codec/xmltab"
	"github.com/viant/xdatly/codec"
	"reflect"
)

const (
	KeyXmltab = "Xmltab"
)

type (
	XmltabFactory struct{}

	Xmltab struct {
		service *xmltab.Service
	}
)

func (e *XmltabFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	ret := &Xmltab{service: xmltab.New()}
	return ret, nil
}

func (e *Xmltab) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&xmltab.Result{}), nil
}

func (e *Xmltab) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)
	return e.service.Transfer(raw)
}
