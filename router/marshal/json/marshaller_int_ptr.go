package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type IntPtrMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewIntPtrMarshaller(dTag *DefaultTag) *IntPtrMarshaller {
	var zeroValue *int
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*int)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(*zeroValue)
	}

	return &IntPtrMarshaller{
		dTag:         dTag,
		defaultValue: zeroString,
	}
}

func (i *IntPtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsIntAddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(**intPtr, sb)
}

func (i *IntPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddIntNull(xunsafe.AsIntAddrPtr(pointer))
}

type Int8PtrMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewInt8PtrMarshaller(tag *DefaultTag) *Int8PtrMarshaller {
	var zeroValue *int8
	if tag._value != nil {
		zeroValue, _ = tag._value.(*int8)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &Int8PtrMarshaller{
		defaultValue: zeroString,
		dTag:         tag,
	}
}

func (i *Int8PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsInt8AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *Int8PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddInt8Null(xunsafe.AsInt8AddrPtr(pointer))
}

type Int16PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewInt16PtrMarshaller(dTag *DefaultTag) *Int16PtrMarshaller {
	var zeroValue *int16
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*int16)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &Int16PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *Int16PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsInt16AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *Int16PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return mainDecoder.AddInt16Null(xunsafe.AsInt16AddrPtr(pointer))
}

type Int32PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewInt32PtrMarshaller(dTag *DefaultTag) *Int32PtrMarshaller {
	var zeroValue *int32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*int32)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &Int32PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *Int32PtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsInt32AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *Int32PtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddInt32Null(xunsafe.AsInt32AddrPtr(pointer))
}

type IntPtr64Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewInt64PtrMarshaller(dTag *DefaultTag) *IntPtr64Marshaller {
	var zeroValue *int64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*int64)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &IntPtr64Marshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *IntPtr64Marshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsInt64AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *IntPtr64Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddInt64Null(xunsafe.AsInt64AddrPtr(pointer))
}
