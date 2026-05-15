package json

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type (
	sliceMarshaller struct {
		xslice     *xunsafe.Slice
		elemType   reflect.Type
		marshaller marshaler
		path       string
	}

	sliceDecoder struct {
		rType        reflect.Type
		ptr          unsafe.Pointer
		appender     *xunsafe.Appender
		unmarshaller marshaler
		session      *UnmarshalSession
	}

	sliceInterfaceMarshaller struct {
		cache      *marshallersCache
		config     *config.IOConfig
		outputPath string
		path       string
		tag        *format.Tag
	}
)

func newSliceMarshaller(rType reflect.Type, config *config.IOConfig, path string, outputPath string, tag *format.Tag, cache *marshallersCache) (marshaler, error) {
	elemType := rType.Elem()

	marshaller, err := cache.loadMarshaller(elemType, config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	return &sliceMarshaller{
		path:       path,
		elemType:   elemType,
		marshaller: marshaller,
		xslice:     xunsafe.NewSlice(rType, xunsafe.UseItemAddrOpt(true)),
	}, err
}

func (s *sliceMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	if skipNull(decoder) {
		return nil
	}
	if pointer != nil {
		hdr := (*reflect.SliceHeader)(pointer)
		if hdr.Data == 0 {
			sliceType := reflect.SliceOf(s.elemType)
			zeroVal := reflect.MakeSlice(sliceType, 0, 0)
			reflect.NewAt(sliceType, pointer).Elem().Set(zeroVal)
		}
	}
	err := decoder.Array(newSliceDecoder(s.elemType, pointer, s.xslice, s.marshaller, session))
	if err != nil {
		return err
	}
	return decoderError(decoder)
}
func (s *sliceMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {

	if fn, ok := sb.Interceptors[s.path]; ok {
		result, err := fn()
		if err != nil {
			return err
		}

		sb.Write(result)
		return nil
	}
	if ptr == nil {
		sb.Write(nullBytes)
		return nil
	}
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
		if valuePtr == nil {
			sb.Write(nullBytes)
			continue
		}
		if err := s.marshaller.MarshallObject(valuePtr, sb); err != nil {
			return err
		}

	}
	sb.WriteByte(']')

	return nil
}

func newSliceDecoder(rType reflect.Type, ptr unsafe.Pointer, xslice *xunsafe.Slice, unmarshaller marshaler, session *UnmarshalSession) *sliceDecoder {
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
	err := s.unmarshaller.UnmarshallObject(xunsafe.AsPointer(add), d, nil, s.session)
	if err != nil {
		l := s.appender.Len()
		return NewError(fmt.Sprintf("[%d]", l-1), err)
	}
	return nil
}

func newSliceInterfaceMarshaller(config *config.IOConfig, path string, outputPath string, tag *format.Tag, cache *marshallersCache) marshaler {
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

		if iface == nil {
			sb.Write(nullBytes)
			continue
		}
		ifaceType := reflect.TypeOf(iface)
		if ifaceType == nil {
			sb.Write(nullBytes)
			continue
		}

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

func (s *sliceInterfaceMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	ifaces := (*[]interface{})(pointer)

	var result interface{}

	// Use Interface() which marks the value as consumed by setting the
	// decoder's `called` flag. Using DecodeInterface() left that flag unset
	// which made go-jay believe the key hadn’t been processed, causing it to
	// skip data and eventually throw errors such as
	//   Invalid JSON, wrong char '}' found at position ...
	if err := decoder.Interface(&result); err != nil {
		return err
	}

	*(ifaces) = append(*(ifaces), result)
	return nil
}
