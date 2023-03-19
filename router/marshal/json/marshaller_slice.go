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
	SliceMarshaller struct {
		xslice     *xunsafe.Slice
		elemType   reflect.Type
		marshaller Marshaler
	}

	sliceDecoder struct {
		rType        reflect.Type
		ptr          unsafe.Pointer
		appender     *xunsafe.Appender
		unmarshaller Marshaler
	}

	SliceInterfaceMarshaller struct {
		cache      *Cache
		config     marshal.Default
		outputPath string
		path       string
		tag        *DefaultTag
	}
)

func NewSliceMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *Cache) (Marshaler, error) {
	elemType := rType.Elem()

	marshaller, err := cache.LoadMarshaller(elemType, config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	return &SliceMarshaller{
		elemType:   elemType,
		marshaller: marshaller,
		xslice:     xunsafe.NewSlice(rType, xunsafe.UseItemAddrOpt(true)),
	}, err
}

func (s *SliceMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return mainDecoder.Array(newSliceDecoder(rType.Elem(), pointer, s.xslice, s.marshaller))
}

func (s *SliceMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters) error {
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

		valuePtr := s.xslice.PointerAt(ptr, uintptr(i))
		if err := s.marshaller.MarshallObject(elemType, valuePtr, sb, filters); err != nil {
			return err
		}

	}
	sb.WriteByte(']')

	return nil
}

func newSliceDecoder(rType reflect.Type, ptr unsafe.Pointer, xslice *xunsafe.Slice, unmarshaller Marshaler) *sliceDecoder {
	return &sliceDecoder{
		rType:        rType,
		ptr:          ptr,
		appender:     xslice.Appender(ptr),
		unmarshaller: unmarshaller,
	}
}

func (s *sliceDecoder) UnmarshalJSONArray(d *gojay.Decoder) error {
	add := s.appender.Add()
	return s.unmarshaller.UnmarshallObject(s.rType, xunsafe.AsPointer(add), d, nil)
}

func NewSliceInterfaceMarshaller(config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *Cache) Marshaler {
	return &SliceInterfaceMarshaller{
		cache:      cache,
		config:     config,
		path:       path,
		outputPath: outputPath,
		tag:        tag,
	}
}

func (s *SliceInterfaceMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters) error {
	ifaces := xunsafe.AsInterfaces(ptr)

	sb.WriteByte('[')
	for i, iface := range ifaces {
		if i != 0 {
			sb.WriteByte(',')
		}

		ifaceType := reflect.TypeOf(iface)
		if ifaceType.Kind() == reflect.Ptr {
			ifaceType = ifaceType.Elem()
		}

		marshaller, err := s.cache.ElemMarshallerIfNeeded(ifaceType, s.config, s.path, s.outputPath, s.tag)
		if err != nil {
			return err
		}

		if err = marshaller.MarshallObject(ifaceType, xunsafe.AsPointer(iface), sb, filters); err != nil {
			return err
		}
	}

	sb.WriteByte(']')
	return nil
}

func (s *SliceInterfaceMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	ifaces := (*[]interface{})(pointer)

	var result interface{}
	if err := decoder.DecodeInterface(&result); err != nil {
		return err
	}

	*(ifaces) = append(*(ifaces), result)
	return nil
}
