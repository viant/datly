package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"unsafe"
)

type Float32Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewFloat32Marshaller(dTag *DefaultTag) *Float32Marshaller {
	var zeroValue float32
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(float32)
	}

	return &Float32Marshaller{
		zeroValue: formatFloat(float64(zeroValue)),
		dTag:      dTag,
	}
}

func formatFloat(zeroValue float64) string {
	return strconv.FormatFloat(zeroValue, 'f', -1, 64)
}

func (i *Float32Marshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asFloat32 := xunsafe.AsFloat32(ptr)
	if asFloat32 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendFloat(float64(asFloat32), false, i.dTag, sb)
}

func (i *Float32Marshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return decoder.AddFloat32((*float32)(pointer))
}

type Float64Marshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewFloat64Marshaller(dTag *DefaultTag) *Float64Marshaller {
	var zeroValue float64
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(float64)
	}

	return &Float64Marshaller{
		zeroValue: formatFloat(zeroValue),
		dTag:      dTag,
	}
}

func (i *Float64Marshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asFloat64 := xunsafe.AsFloat64(ptr)
	if asFloat64 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendFloat(asFloat64, false, i.dTag, sb)
}

func (i *Float64Marshaller) UnmarshallObject(_ reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, _ *gojay.Decoder) error {
	return decoder.AddFloat64((*float64)((pointer)))
}

func appendFloat(f float64, wasNull bool, tag *DefaultTag, sb *Session) error {
	if wasNull {
		sb.WriteString(null)
		return nil
	}

	if f == 0 && tag._value != nil {
		sb.WriteString(strconv.FormatFloat(tag._value.(float64), 'f', -1, 64))
		return nil
	}

	if wasNull {
		sb.WriteString("0")
		return nil
	}

	sb.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
	return nil
}
