package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type intMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func newIntMarshaller(dTag *DefaultTag) *intMarshaller {
	var zeroValue int
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(int)
	}

	return &intMarshaller{
		dTag:         dTag,
		defaultValue: strconv.Itoa(zeroValue),
	}
}

func (i *intMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asInt := xunsafe.AsInt(ptr)
	if asInt == 0 {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(asInt, sb)
}

func (i *intMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddInt(xunsafe.AsIntPtr(pointer))
}

type int8Marshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewInt8Marshaller(tag *DefaultTag) *int8Marshaller {
	var zeroValue int8
	if tag._value != nil {
		zeroValue, _ = tag._value.(int8)
	}

	return &int8Marshaller{
		defaultValue: strconv.Itoa(int(zeroValue)),
		dTag:         tag,
	}
}

func (i *int8Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asInt8 := xunsafe.AsInt8(ptr)
	if asInt8 == 0 {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(asInt8), sb)
}

func (i *int8Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddInt8((*int8)(pointer))
}

type int16Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newInt16Marshaller(dTag *DefaultTag) *int16Marshaller {
	var zeroValue int16
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(int16)
	}

	return &int16Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *int16Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asInt16 := xunsafe.AsInt16(ptr)
	if asInt16 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asInt16), sb)
}

func (i *int16Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddInt16((*int16)(pointer))
}

type int32Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newInt32Marshaller(dTag *DefaultTag) *int32Marshaller {
	var zeroValue int32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(int32)
	}

	return &int32Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *int32Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asInt32 := xunsafe.AsInt32(ptr)
	if asInt32 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asInt32), sb)
}

func (i *int32Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddInt32((*int32)((pointer)))
}

type int64Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newInt64Marshaller(dTag *DefaultTag) *int64Marshaller {
	var zeroValue int64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(int64)
	}

	return &int64Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *int64Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asInt64 := xunsafe.AsInt64(ptr)
	if asInt64 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asInt64), sb)
}

func (i *int64Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddInt64((*int64)(pointer))
}

func appendInt(value int, sb *MarshallSession) error {
	sb.Buffer.Grow(64)
	dst := sb.Bytes()[sb.Len():]
	parsed := strconv.AppendInt(dst, int64(value), 10)
	sb.Write(parsed)
	return nil
}
