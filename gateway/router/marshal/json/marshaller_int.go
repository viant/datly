package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type intMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newIntMarshaller(dTag *format.Tag) *intMarshaller {
	var zeroValue = "0"
	if dTag.IsNullable() {
		zeroValue = null
	}
	return &intMarshaller{
		dTag:         dTag,
		defaultValue: zeroValue,
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
	dTag         *format.Tag
}

func NewInt8Marshaller(tag *format.Tag) *int8Marshaller {
	return &int8Marshaller{
		defaultValue: intZeroValue(tag),
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
	dTag      *format.Tag
}

func newInt16Marshaller(dTag *format.Tag) *int16Marshaller {
	return &int16Marshaller{
		zeroValue: intZeroValue(dTag),
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
	dTag      *format.Tag
}

func newInt32Marshaller(dTag *format.Tag) *int32Marshaller {
	return &int32Marshaller{
		zeroValue: intZeroValue(dTag),
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
	dTag      *format.Tag
}

func newInt64Marshaller(dTag *format.Tag) *int64Marshaller {
	var zeroValue = "0"
	if dTag.IsNullable() {
		zeroValue = null
	}
	return &int64Marshaller{
		zeroValue: zeroValue,
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

func intZeroValue(dTag *format.Tag) string {
	var zeroValue = "0"
	if dTag.IsNullable() {
		zeroValue = null
	}
	return zeroValue
}
