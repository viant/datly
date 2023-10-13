package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/structology/format"
	"github.com/viant/xunsafe"
	"unsafe"
)

type float32PtrMarshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newFloat32PtrMarshaller(dTag *format.Tag) *float32PtrMarshaller {
	var zeroValue = "0.0"
	if dTag.IsNullable() {
		zeroValue = null
	}

	return &float32PtrMarshaller{
		zeroValue: zeroValue,
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

func (i *float32PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddFloat32Null(xunsafe.AsFloat32AddrPtr(pointer))
}

type float64PtrMarshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newFloat64PtrMarshaller(dTag *format.Tag) *float64PtrMarshaller {
	var zeroValue = "0.0"
	if dTag.IsNullable() {
		zeroValue = null
	}
	return &float64PtrMarshaller{
		zeroValue: zeroValue,
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

func (i *float64PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddFloat64Null(xunsafe.AsFloat64AddrPtr(pointer))
}
