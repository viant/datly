package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/structology/format"
	"github.com/viant/xunsafe"
	"unsafe"
)

type intPtrMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newIntPtrMarshaller(dTag *format.Tag) *intPtrMarshaller {
	return &intPtrMarshaller{
		dTag:         dTag,
		defaultValue: intPtrDefaultValue(dTag),
	}
}

func (i *intPtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsIntAddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(**intPtr, sb)
}

func (i *intPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddIntNull(xunsafe.AsIntAddrPtr(pointer))
}

type nnt8PtrMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newInt8PtrMarshaller(dTag *format.Tag) *nnt8PtrMarshaller {
	return &nnt8PtrMarshaller{
		defaultValue: intPtrDefaultValue(dTag),
		dTag:         dTag,
	}
}

func (i *nnt8PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsInt8AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *nnt8PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddInt8Null(xunsafe.AsInt8AddrPtr(pointer))
}

type int16PtrMarshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newInt16PtrMarshaller(dTag *format.Tag) *int16PtrMarshaller {
	return &int16PtrMarshaller{
		zeroValue: intPtrDefaultValue(dTag),
		dTag:      dTag,
	}
}

func intPtrDefaultValue(dTag *format.Tag) string {
	defaultValue := null
	if !dTag.IsNullable() {
		defaultValue = "0"
	}
	return defaultValue
}

func (i *int16PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsInt16AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *int16PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddInt16Null(xunsafe.AsInt16AddrPtr(pointer))
}

type int32PtrMarshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newInt32PtrMarshaller(dTag *format.Tag) *int32PtrMarshaller {
	return &int32PtrMarshaller{
		zeroValue: intPtrDefaultValue(dTag),
		dTag:      dTag,
	}
}

func (i *int32PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsInt32AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *int32PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddInt32Null(xunsafe.AsInt32AddrPtr(pointer))
}

type intPtr64Marshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newInt64PtrMarshaller(dTag *format.Tag) *intPtr64Marshaller {
	return &intPtr64Marshaller{
		zeroValue: intPtrDefaultValue(dTag),
		dTag:      dTag,
	}
}

func (i *intPtr64Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsInt64AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *intPtr64Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddInt64Null(xunsafe.AsInt64AddrPtr(pointer))
}
