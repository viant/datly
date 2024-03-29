package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type (
	inlinableMarshaller struct {
		accessor  *xunsafe.Field
		rType     reflect.Type
		marshaler marshaler
		isIface   bool
	}
)

func newInlinableMarshaller(field reflect.StructField, config *config.IOConfig, path, outputPath string, dTag *format.Tag, cache *marshallersCache) (*inlinableMarshaller, error) {
	marshaler, err := cache.loadMarshaller(field.Type, config, path, outputPath, dTag)
	if err != nil {
		return nil, err
	}

	return &inlinableMarshaller{
		marshaler: marshaler,
		accessor:  xunsafe.NewField(field),
		isIface:   field.Type.Kind() == reflect.Interface,
		rType:     field.Type,
	}, nil
}

func (i *inlinableMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	value := i.accessor.Value(ptr)
	pointer := AsPtr(value, i.rType)
	return i.marshaler.MarshallObject(pointer, sb)
}

func (i *inlinableMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	aValue := i.accessor.Value(pointer)
	pointer = AsPtr(aValue, i.rType)
	return i.marshaler.UnmarshallObject(pointer, decoder, auxiliaryDecoder, session)
}
