package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type (
	sliceMarshaller struct {
		xslice     *xunsafe.Slice
		elemType   reflect.Type
		marshaller Marshaler
	}

	sliceDecoder struct {
		rType        reflect.Type
		ptr          unsafe.Pointer
		appender     *xunsafe.Appender
		unmarshaller Marshaler
		session      *UnmarshallSession
	}

	sliceInterfaceMarshaller struct {
		cache      *marshallersCache
		config     marshal.Default
		outputPath string
		path       string
		tag        *DefaultTag
	}
)

func newSliceMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *marshallersCache) (Marshaler, error) {
	elemType := rType.Elem()

	marshaller, err := cache.loadMarshaller(elemType, config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	return &sliceMarshaller{
		elemType:   elemType,
		marshaller: marshaller,
		xslice:     xunsafe.NewSlice(rType, xunsafe.UseItemAddrOpt(true)),
	}, err
}

func (s *sliceMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddArray(newSliceDecoder(s.elemType, pointer, s.xslice, s.marshaller, session))
}

func (s *sliceMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	sliceHeader := (*reflect.SliceHeader)(ptr)
	if s != nil && sliceHeader.Data == 0 {
		sb.WriteString("[]")
		return nil
	}

	sb.WriteByte('[')
	sliceLen := s.xslice.Len(ptr)
	for i := 0; i < sliceLen; i++ {
		if i != 0 {
			sb.WriteByte(',')
		}

		valuePtr := s.xslice.PointerAt(ptr, uintptr(i))
		if err := s.marshaller.MarshallObject(valuePtr, sb); err != nil {
			return err
		}

	}
	sb.WriteByte(']')

	return nil
}

func newSliceDecoder(rType reflect.Type, ptr unsafe.Pointer, xslice *xunsafe.Slice, unmarshaller Marshaler, session *UnmarshallSession) *sliceDecoder {
	return &sliceDecoder{
		rType:        rType,
		ptr:          ptr,
		appender:     xslice.Appender(ptr),
		unmarshaller: unmarshaller,
		session:      session,
	}
}

func (s *sliceDecoder) UnmarshalJSONArray(d *gojay.Decoder) error {
	add := s.appender.Add()
	return s.unmarshaller.UnmarshallObject(xunsafe.AsPointer(add), d, nil, s.session)
}

func newSliceInterfaceMarshaller(config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *marshallersCache) Marshaler {
	return &sliceInterfaceMarshaller{
		cache:      cache,
		config:     config,
		path:       path,
		outputPath: outputPath,
		tag:        tag,
	}
}

func (s *sliceInterfaceMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	ifaces := xunsafe.AsInterfaces(ptr)

	sb.WriteByte('[')
	for i, iface := range ifaces {
		if i != 0 {
			sb.WriteByte(',')
		}

		ifaceType := reflect.TypeOf(iface)

		marshaller, err := s.cache.loadMarshaller(ifaceType, s.config, s.path, s.outputPath, s.tag)
		if err != nil {
			return err
		}

		pointer := AsPtr(iface, ifaceType)

		if err = marshaller.MarshallObject(pointer, sb); err != nil {
			return err
		}
	}

	sb.WriteByte(']')
	return nil
}

func (s *sliceInterfaceMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	ifaces := (*[]interface{})(pointer)

	var result interface{}
	if err := decoder.DecodeInterface(&result); err != nil {
		return err
	}

	*(ifaces) = append(*(ifaces), result)
	return nil
}
