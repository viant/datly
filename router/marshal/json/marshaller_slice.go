package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type SliceMarshaller struct {
	isInterfaceSlice bool
	xslice           *xunsafe.Slice
	elemType         reflect.Type
	marshaller       Marshaler
}

func NewSliceMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *Cache) (*SliceMarshaller, error) {
	elemType := rType.Elem()
	marshaller, err := cache.LoadMarshaller(elemType, config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	return &SliceMarshaller{
		elemType:         elemType,
		marshaller:       marshaller,
		isInterfaceSlice: elemType.Kind() == reflect.Interface,
		xslice:           xunsafe.NewSlice(rType),
	}, err
}

func (s *SliceMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
	return mainDecoder.Array(newSliceDecoder(rType.Elem(), pointer, s.xslice, s.marshaller.UnmarshallObject))
}

func (s *SliceMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters, opts ...Option) error {
	sliceHeader := (*reflect.SliceHeader)(ptr)
	if s != nil && sliceHeader.Data == 0 {
		sb.WriteString("[]")
		return nil
	}

	elemType := s.elemType
	sb.WriteByte('[')
	sliceLen := s.xslice.Len(ptr)
	for i := 0; i < sliceLen; i++ {
		if i != 0 {
			sb.WriteByte(',')
		}
		valueAt := s.xslice.ValueAt(ptr, i)
		valuePtr := xunsafe.AsPointer(valueAt)
		if s.isInterfaceSlice {
			elemType = reflect.TypeOf(valueAt)
		}
		if err := s.marshaller.MarshallObject(elemType, valuePtr, sb, filters); err != nil {
			return err
		}

	}
	sb.WriteByte(']')

	return nil
}
