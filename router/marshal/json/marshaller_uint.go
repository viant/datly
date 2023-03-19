package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"unsafe"
)

type UintMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewUintMarshaller(dTag *DefaultTag) *UintMarshaller {
	var zeroValue uint
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(uint)
	}

	return &UintMarshaller{
		dTag:         dTag,
		defaultValue: strconv.Itoa(int(zeroValue)),
	}
}

func (i *UintMarshaller) MarshallObject(_ reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asUint := xunsafe.AsUint(ptr)
	if asUint == 0 {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(asUint), sb)
}

func (i *UintMarshaller) UnmarshallObject(_ reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddUint64(xunsafe.AsUint64Ptr(pointer))
}

type Uint8Marshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewUint8Marshaller(tag *DefaultTag) *Uint8Marshaller {
	var zeroValue uint8
	if tag._value != nil {
		zeroValue, _ = tag._value.(uint8)
	}

	return &Uint8Marshaller{
		defaultValue: strconv.Itoa(int(zeroValue)),
		dTag:         tag,
	}
}

func (i *Uint8Marshaller) MarshallObject(_ reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asUint8 := xunsafe.AsUint8(ptr)
	if asUint8 == 0 {
		sb.WriteString(i.defaultValue)
		return nil
	}

	return appendInt(int(asUint8), sb)
}

func (i *Uint8Marshaller) UnmarshallObject(_ reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddUint8((*uint8)(pointer))
}

type Uint16Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewUint16Marshaller(dTag *DefaultTag) *Uint16Marshaller {
	var zeroValue uint16
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(uint16)
	}

	return &Uint16Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *Uint16Marshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asUint16 := xunsafe.AsUint16(ptr)
	if asUint16 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asUint16), sb)
}

func (i *Uint16Marshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return mainDecoder.AddUint16((*uint16)(pointer))
}

type Uint32Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewUint32Marshaller(dTag *DefaultTag) *Uint32Marshaller {
	var zeroValue uint32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(uint32)
	}

	return &Uint32Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *Uint32Marshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asUint32 := xunsafe.AsUint32(ptr)
	if asUint32 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asUint32), sb)
}

func (i *Uint32Marshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddUint32((*uint32)((pointer)))
}

type Uint64Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewUint64Marshaller(dTag *DefaultTag) *Uint64Marshaller {
	var zeroValue uint64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(uint64)
	}

	return &Uint64Marshaller{
		zeroValue: strconv.Itoa(int(zeroValue)),
		dTag:      dTag,
	}
}

func (i *Uint64Marshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asUint64 := xunsafe.AsUint64(ptr)
	if asUint64 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendInt(int(asUint64), sb)
}

func (i *Uint64Marshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddUint64((*uint64)((pointer)))
}
