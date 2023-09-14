package codec

import (
	"context"
	"github.com/viant/datly/config/codec/xmljob"
	"github.com/viant/xdatly/codec"
	"reflect"
)

const (
	KeyXmlJob = "XmlJob"
)

type (
	XmlJobFactory struct{}

	XmlJob struct {
		service *xmljob.Service
	}
)

func (e *XmlJobFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	ret := &XmlJob{service: xmljob.New()}
	return ret, nil
}

func (e *XmlJob) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&xmljob.Job{}), nil
}

func (e *XmlJob) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)
	return e.service.Transfer(raw)
}
