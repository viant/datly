package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/structology/format"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type float32Marshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newFloat32Marshaller(dTag *format.Tag) *float32Marshaller {
	var zeroValue = "0.0"
	if dTag.IsNullable() {
		zeroValue = null
	}
	return &float32Marshaller{
		zeroValue: zeroValue,
		dTag:      dTag,
	}
}

func formatFloat(zeroValue float64) string {
	return strconv.FormatFloat(zeroValue, 'f', -1, 64)
}

func (i *float32Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asFloat32 := xunsafe.AsFloat32(ptr)
	if asFloat32 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendFloat(float64(asFloat32), sb)
}

func (i *float32Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddFloat32((*float32)(pointer))
}

type float64Marshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newFloat64Marshaller(dTag *format.Tag) *float64Marshaller {
	var zeroValue = "0.0"
	if dTag.IsNullable() {
		zeroValue = null
	}

	return &float64Marshaller{
		zeroValue: zeroValue,
		dTag:      dTag,
	}
}

func (i *float64Marshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asFloat64 := xunsafe.AsFloat64(ptr)
	if asFloat64 == 0 {
		sb.WriteString(i.zeroValue)
		return nil
	}

	return appendFloat(asFloat64, sb)
}

func (i *float64Marshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddFloat64((*float64)((pointer)))
}

func appendFloat(f float64, sb *MarshallSession) error {
	sb.Buffer.Grow(64)
	dst := sb.Bytes()[sb.Len():]
	parsed := strconv.AppendFloat(dst, f, 'f', -1, 64)
	sb.Write(parsed)
	//sb.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
	return nil
}
