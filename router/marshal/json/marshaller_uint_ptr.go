package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type uintPtrMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func newUintPtrMarshaller(dTag *DefaultTag) *uintPtrMarshaller {
	var zeroValue *uint
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*uint)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &uintPtrMarshaller{
		dTag:         dTag,
		defaultValue: zeroString,
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
	dTag         *DefaultTag
}

func newUint8PtrMarshaller(tag *DefaultTag) *uint8PtrMarshaller {
	var zeroValue *uint8
	if tag._value != nil {
		zeroValue, _ = tag._value.(*uint8)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &uint8PtrMarshaller{
		defaultValue: zeroString,
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
	zeroValue string
	dTag      *DefaultTag
}

func newUint16PtrMarshaller(dTag *DefaultTag) *uint16PtrMarshaller {
	var zeroValue *uint16
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*uint16)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &uint16PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *uint16PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsUint16AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *uint16PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint16Null(xunsafe.AsUint16AddrPtr(pointer))
}

type uint32PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newUint32PtrMarshaller(dTag *DefaultTag) *uint32PtrMarshaller {
	var zeroValue *uint32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*uint32)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &uint32PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *uint32PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsUint32AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *uint32PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint32Null(xunsafe.AsUint32AddrPtr(pointer))
}

type uint64PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newUint64PtrMarshaller(dTag *DefaultTag) *uint64PtrMarshaller {
	var zeroValue *uint64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*uint64)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &uint64PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *uint64PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsUint64AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *uint64PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint64Null(xunsafe.AsUint64AddrPtr(pointer))
}
