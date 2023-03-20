package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type (
	InlinableMarshaller struct {
		accessor  *xunsafe.Field
		rType     reflect.Type
		marshaler Marshaler
		isIface   bool
	}
)

func NewInlinableMarshaller(field reflect.StructField, config marshal.Default, path, outputPath string, dTag *DefaultTag, cache *Cache) (*InlinableMarshaller, error) {
	marshaler, err := cache.ElemMarshallerIfNeeded(field.Type, config, path, outputPath, dTag)
	if err != nil {
		return nil, err
	}

	return &InlinableMarshaller{
		marshaler: marshaler,
		accessor:  xunsafe.NewField(field),
		isIface:   field.Type.Kind() == reflect.Interface,
		rType:     field.Type,
	}, nil
}

func (i *InlinableMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	value := i.accessor.Value(ptr)
	if i.isIface {
		rType = reflect.TypeOf(value)
	}

	return i.marshaler.MarshallObject(rType, xunsafe.AsPointer(value), sb)
}

func (i *InlinableMarshaller) UnmarshallObject(rType reflect.Type, ptr unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	aValue := i.accessor.Value(ptr)
	if i.isIface {
		rType = reflect.TypeOf(aValue)
	}

	return i.marshaler.UnmarshallObject(rType, xunsafe.AsPointer(aValue), mainDecoder, nullDecoder)
}
