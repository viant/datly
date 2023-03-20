package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type Float32PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewFloat32PtrMarshaller(dTag *DefaultTag) *Float32PtrMarshaller {
	var zeroValue *float32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*float32)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = formatFloat(float64(*zeroValue))
	}

	return &Float32PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *Float32PtrMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	f32Ptr := xunsafe.AsFloat32AddrPtr(ptr)
	if f32Ptr == nil || *f32Ptr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendFloat(float64(**f32Ptr), false, i.dTag, sb)
}

func (i *Float32PtrMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddFloat32Null(xunsafe.AsFloat32AddrPtr(pointer))
}

type Float64PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewFloat64PtrMarshaller(dTag *DefaultTag) *Float64PtrMarshaller {
	var zeroValue *float64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*float64)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = formatFloat(*zeroValue)
	}

	return &Float64PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *Float64PtrMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	f32Ptr := xunsafe.AsFloat64AddrPtr(ptr)
	if f32Ptr == nil || *f32Ptr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendFloat(**f32Ptr, false, i.dTag, sb)
}

func (i *Float64PtrMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddFloat64Null(xunsafe.AsFloat64AddrPtr(pointer))
}
