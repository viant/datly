package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type uintMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func newUintMarshaller(dTag *DefaultTag) *uintMarshaller {
	var zeroValue uint
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(uint)
	}

	return &uintMarshaller{
		dTag:         dTag,
		defaultValue: strconv.Itoa(int(zeroValue)),
	}
}

func (i *uintMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asUint := xunsafe.AsUint(ptr)
	if asUint == 0 {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(asUint), sb)
}

func (i *uintMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddUint64(xunsafe.AsUint64Ptr(pointer))
}

type uint8Marshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func newUint8Marshaller(tag *DefaultTag) *uint8Marshaller {
	var zeroValue uint8
	if tag._value != nil {
		zeroValue, _ = tag._value.(uint8)
	}

	return &uint8Marshaller{
		defaultValue: strconv.Itoa(int(zeroValue)),
		dTag:         tag,
	}
}

func (i *uint8Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asUint8 := xunsafe.AsUint8(ptr)
	if asUint8 == 0 {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(asUint8), sb)
}

func (i *uint8Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddUint8((*uint8)(pointer))
}

type uint16Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newUint16Marshaller(dTag *DefaultTag) *uint16Marshaller {
	var zeroValue uint16
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(uint16)
	}

	return &uint16Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *uint16Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asUint16 := xunsafe.AsUint16(ptr)
	if asUint16 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asUint16), sb)
}

func (i *uint16Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddUint16((*uint16)(pointer))
}

type uint32Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newUint32Marshaller(dTag *DefaultTag) *uint32Marshaller {
	var zeroValue uint32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(uint32)
	}

	return &uint32Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *uint32Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asUint32 := xunsafe.AsUint32(ptr)
	if asUint32 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asUint32), sb)
}

func (i *uint32Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddUint32((*uint32)((pointer)))
}

type uint64Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newUint64Marshaller(dTag *DefaultTag) *uint64Marshaller {
	var zeroValue uint64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(uint64)
	}

	return &uint64Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *uint64Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asUint64 := xunsafe.AsUint64(ptr)
	if asUint64 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asUint64), sb)
}

func (i *uint64Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddUint64((*uint64)((pointer)))
}
