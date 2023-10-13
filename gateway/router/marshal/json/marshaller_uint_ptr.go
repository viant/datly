package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/structology/format"
	"github.com/viant/xunsafe"
	"unsafe"
)

type uintPtrMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newUintPtrMarshaller(dTag *format.Tag) *uintPtrMarshaller {
	return &uintPtrMarshaller{
		dTag:         dTag,
		defaultValue: intPtrDefaultValue(dTag),
	}
}

func (i *uintPtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsUintAddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *uintPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint64Null(xunsafe.AsUint64AddrPtr(pointer))
}

type uint8PtrMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newUint8PtrMarshaller(tag *format.Tag) *uint8PtrMarshaller {
	return &uint8PtrMarshaller{
		defaultValue: intPtrDefaultValue(tag),
		dTag:         tag,
	}
}

func (i *uint8PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsUint8AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *uint8PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint8Null(xunsafe.AsUint8AddrPtr(pointer))
}

type uint16PtrMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newUint16PtrMarshaller(dTag *format.Tag) *uint16PtrMarshaller {
	return &uint16PtrMarshaller{
		defaultValue: intPtrDefaultValue(dTag),
		dTag:         dTag,
	}
}

func (i *uint16PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsUint16AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *uint16PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint16Null(xunsafe.AsUint16AddrPtr(pointer))
}

type uint32PtrMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newUint32PtrMarshaller(dTag *format.Tag) *uint32PtrMarshaller {
	return &uint32PtrMarshaller{
		defaultValue: intPtrDefaultValue(dTag),
		dTag:         dTag,
	}
}

func (i *uint32PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsUint32AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *uint32PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint32Null(xunsafe.AsUint32AddrPtr(pointer))
}

type uint64PtrMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newUint64PtrMarshaller(dTag *format.Tag) *uint64PtrMarshaller {
	return &uint64PtrMarshaller{
		defaultValue: intPtrDefaultValue(dTag),
		dTag:         dTag,
	}
}

func (i *uint64PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsUint64AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *uint64PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint64Null(xunsafe.AsUint64AddrPtr(pointer))
}
