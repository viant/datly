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
		accessor  *xunsafe.Field
		rType     reflect.Type
		marshaler Marshaler
	}
)

func NewInlinableMarshaller(field reflect.StructField, config marshal.Default, path, outputPath string, dTag *DefaultTag, cache *Cache) (*InlinableMarshaller, error) {
	fieldType := field.Type
	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}

	marshaler, err := cache.LoadMarshaller(fieldType, config, path, outputPath, dTag)
	if err != nil {
		return nil, err
	}

	return &InlinableMarshaller{
		marshaler: marshaler,
		accessor:  xunsafe.NewField(field),
		rType:     field.Type,
	}, nil
}

func (i *InlinableMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters, opts ...MarshallOption) error {
	value := i.accessor.Value(ptr)
	return i.marshaler.MarshallObject(i.rType, xunsafe.AsPointer(value), sb, filters)
}

func (i *InlinableMarshaller) UnmarshallObject(rType reflect.Type, ptr unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	aValue := i.accessor.Value(ptr)
	return i.marshaler.UnmarshallObject(rType, xunsafe.AsPointer(aValue), mainDecoder, nullDecoder)
}
