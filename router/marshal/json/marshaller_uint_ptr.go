package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"unsafe"
)

type UintPtrMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewUintPtrMarshaller(dTag *DefaultTag) *UintPtrMarshaller {
	var zeroValue *uint
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*uint)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &UintPtrMarshaller{
		dTag:         dTag,
		defaultValue: zeroString,
	}
}

func (i *UintPtrMarshaller) MarshallObject(_ reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsUintAddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *UintPtrMarshaller) UnmarshallObject(_ reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddUint64Null(xunsafe.AsUint64AddrPtr(pointer))
}

type Uint8PtrMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewUint8PtrMarshaller(tag *DefaultTag) *Uint8PtrMarshaller {
	var zeroValue *uint8
	if tag._value != nil {
		zeroValue, _ = tag._value.(*uint8)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &Uint8PtrMarshaller{
		defaultValue: zeroString,
		dTag:         tag,
	}
}

func (i *Uint8PtrMarshaller) MarshallObject(_ reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsUint8AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *Uint8PtrMarshaller) UnmarshallObject(_ reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddUint8Null(xunsafe.AsUint8AddrPtr(pointer))
}

type Uint16PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewUint16PtrMarshaller(dTag *DefaultTag) *Uint16PtrMarshaller {
	var zeroValue *uint16
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*uint16)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &Uint16PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *Uint16PtrMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsUint16AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *Uint16PtrMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return mainDecoder.AddUint16Null(xunsafe.AsUint16AddrPtr(pointer))
}

type Uint32PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewUint32PtrMarshaller(dTag *DefaultTag) *Uint32PtrMarshaller {
	var zeroValue *uint32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*uint32)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &Uint32PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *Uint32PtrMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsUint32AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *Uint32PtrMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddUint32Null(xunsafe.AsUint32AddrPtr(pointer))
}

type Uint64PtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewUint64PtrMarshaller(dTag *DefaultTag) *Uint64PtrMarshaller {
	var zeroValue *uint64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*uint64)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Itoa(int(*zeroValue))
	}

	return &Uint64PtrMarshaller{
		zeroValue: zeroString,
		dTag:      dTag,
	}
}

func (i *Uint64PtrMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	intPtr := xunsafe.AsUint64AddrPtr(ptr)
	if intPtr == nil || *intPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(**intPtr), sb)
}

func (i *Uint64PtrMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddUint64Null(xunsafe.AsUint64AddrPtr(pointer))
}
