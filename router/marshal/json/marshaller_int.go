package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"unsafe"
)

type IntMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewIntMarshaller(dTag *DefaultTag) *IntMarshaller {
	var zeroValue int
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(int)
	}

	return &IntMarshaller{
		dTag:         dTag,
		defaultValue: strconv.Itoa(zeroValue),
	}
}

func (i *IntMarshaller) MarshallObject(_ reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asInt := xunsafe.AsInt(ptr)
	if asInt == 0 {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(asInt, sb)
}

func (i *IntMarshaller) UnmarshallObject(_ reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddInt(xunsafe.AsIntPtr(pointer))
}

type Int8Marshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewInt8Marshaller(tag *DefaultTag) *Int8Marshaller {
	var zeroValue int8
	if tag._value != nil {
		zeroValue, _ = tag._value.(int8)
	}

	return &Int8Marshaller{
		defaultValue: strconv.Itoa(int(zeroValue)),
		dTag:         tag,
	}
}

func (i *Int8Marshaller) MarshallObject(_ reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asInt8 := xunsafe.AsInt8(ptr)
	if asInt8 == 0 {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(asInt8), sb)
}

func (i *Int8Marshaller) UnmarshallObject(_ reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddInt8((*int8)(pointer))
}

type Int16Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewInt16Marshaller(dTag *DefaultTag) *Int16Marshaller {
	var zeroValue int16
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(int16)
	}

	return &Int16Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *Int16Marshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asInt16 := xunsafe.AsInt16(ptr)
	if asInt16 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asInt16), sb)
}

func (i *Int16Marshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return mainDecoder.AddInt16((*int16)(pointer))
}

type Int32Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewInt32Marshaller(dTag *DefaultTag) *Int32Marshaller {
	var zeroValue int32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(int32)
	}

	return &Int32Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *Int32Marshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asInt32 := xunsafe.AsInt32(ptr)
	if asInt32 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asInt32), sb)
}

func (i *Int32Marshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddInt32((*int32)((pointer)))
}

type Int64Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewInt64Marshaller(dTag *DefaultTag) *Int64Marshaller {
	var zeroValue int64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(int64)
	}

	return &Int64Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *Int64Marshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asInt64 := xunsafe.AsInt64(ptr)
	if asInt64 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asInt64), sb)
}

func (i *Int64Marshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddInt64((*int64)(pointer))
}

func appendInt(value int, sb *Session) error {
	//dest := sb.Next(64)
	//appended := strconv.AppendInt(dest, int64(value), 10)
	sb.WriteString(strconv.Itoa(value))
	return nil
}
