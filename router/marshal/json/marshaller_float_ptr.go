package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"unsafe"
)

type float32PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newFloat32PtrMarshaller(dTag *DefaultTag) *float32PtrMarshaller {
	var zeroValue *float32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*float32)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = formatFloat(float64(*zeroValue))
	}

	return &float32PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *float32PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	f32Ptr := xunsafe.AsFloat32AddrPtr(ptr)
	if f32Ptr == nil || *f32Ptr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendFloat(float64(**f32Ptr), sb)
}

func (i *float32PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddFloat32Null(xunsafe.AsFloat32AddrPtr(pointer))
}

type float64PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newFloat64PtrMarshaller(dTag *DefaultTag) *float64PtrMarshaller {
	var zeroValue *float64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*float64)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = formatFloat(*zeroValue)
	}

	return &float64PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *float64PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	f32Ptr := xunsafe.AsFloat64AddrPtr(ptr)
	if f32Ptr == nil || *f32Ptr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendFloat(**f32Ptr, sb)
}

func (i *float64PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddFloat64Null(xunsafe.AsFloat64AddrPtr(pointer))
}
