package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"unsafe"
)

type uintMarshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newUintMarshaller(dTag *format.Tag) *uintMarshaller {
	return &uintMarshaller{
		dTag:      dTag,
		zeroValue: intZeroValue(dTag),
	}
}

func (i *uintMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asUint := xunsafe.AsUint(ptr)
	if asUint == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asUint), sb)
}

func (i *uintMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint64(xunsafe.AsUint64Ptr(pointer))
}

type uint8Marshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newUint8Marshaller(tag *format.Tag) *uint8Marshaller {
	return &uint8Marshaller{
		zeroValue: intZeroValue(tag),
		dTag:      tag,
	}
}

func (i *uint8Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asUint8 := xunsafe.AsUint8(ptr)
	if asUint8 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asUint8), sb)
}

func (i *uint8Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint8((*uint8)(pointer))
}

type uint16Marshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newUint16Marshaller(dTag *format.Tag) *uint16Marshaller {
	return &uint16Marshaller{
		zeroValue: intZeroValue(dTag),
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

func (i *uint16Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint16((*uint16)(pointer))
}

type uint32Marshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newUint32Marshaller(dTag *format.Tag) *uint32Marshaller {

	return &uint32Marshaller{
		zeroValue: intZeroValue(dTag),
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

func (i *uint32Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint32((*uint32)((pointer)))
}

type uint64Marshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newUint64Marshaller(dTag *format.Tag) *uint64Marshaller {
	return &uint64Marshaller{
		zeroValue: intZeroValue(dTag),
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

func (i *uint64Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddUint64((*uint64)((pointer)))
}
