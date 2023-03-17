package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type (
	InlinableMarshaller struct {
		marshaller   marshallFieldFn
		unmarshaller unmarshallFieldFn
		accessor     *xunsafe.Field
		rType        reflect.Type
	}
)

func (j *Marshaller) NewInlinableMarshaller(field reflect.StructField, config marshal.Default, path string) (*InlinableMarshaller, error) {
	marshaller, unmarshaller := j.storeOrLoadMarshaller(config, path)

	return &InlinableMarshaller{
		marshaller:   marshaller,
		accessor:     xunsafe.NewField(field),
		unmarshaller: unmarshaller,
		rType:        field.Type,
	}, nil
}

func (i *InlinableMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters) error {
	ptr = i.accessor.Pointer(ptr)
	return i.marshaller(i.rType, ptr, sb, filters)
}

func (i *InlinableMarshaller) UnmarshallObject(rType reflect.Type, ptr unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	ptr = i.accessor.Pointer(ptr)
	return i.unmarshaller(rType, ptr, mainDecoder, nullDecoder)
}
