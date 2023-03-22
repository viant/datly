package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type intPtrMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func newIntPtrMarshaller(dTag *DefaultTag) *intPtrMarshaller {
	var zeroValue *int
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*int)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(*zeroValue)
	}

	return &intPtrMarshaller{
		dTag:         dTag,
		defaultValue: zeroString,
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

func (i *intPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddIntNull(xunsafe.AsIntAddrPtr(pointer))
}

type nnt8PtrMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func newInt8PtrMarshaller(tag *DefaultTag) *nnt8PtrMarshaller {
	var zeroValue *int8
	if tag._value != nil {
		zeroValue, _ = tag._value.(*int8)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &nnt8PtrMarshaller{
		defaultValue: zeroString,
		dTag:         tag,
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

func (i *nnt8PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddInt8Null(xunsafe.AsInt8AddrPtr(pointer))
}

type int16PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newInt16PtrMarshaller(dTag *DefaultTag) *int16PtrMarshaller {
	var zeroValue *int16
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*int16)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &int16PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *int16PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	intPtr := xunsafe.AsInt16AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *int16PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddInt16Null(xunsafe.AsInt16AddrPtr(pointer))
}

type int32PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newInt32PtrMarshaller(dTag *DefaultTag) *int32PtrMarshaller {
	var zeroValue *int32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*int32)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &int32PtrMarshaller{
		zeroValue: zeroString,
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

func (i *int32PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddInt32Null(xunsafe.AsInt32AddrPtr(pointer))
}

type intPtr64Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newInt64PtrMarshaller(dTag *DefaultTag) *intPtr64Marshaller {
	var zeroValue *int64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*int64)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &intPtr64Marshaller{
		zeroValue: zeroString,
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

func (i *intPtr64Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddInt64Null(xunsafe.AsInt64AddrPtr(pointer))
}
