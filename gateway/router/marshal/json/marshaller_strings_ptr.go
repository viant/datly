package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/structology/format"
	"github.com/viant/xunsafe"
	"unsafe"
)

type stringPtrMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newStringPtrMarshaller(dTag *format.Tag) *stringPtrMarshaller {
	var zeroValue = `""`
	if dTag.IsNullable() {
		zeroValue = null
	}
	return &stringPtrMarshaller{
		dTag:         dTag,
		defaultValue: zeroValue,
	}
}

func (i *stringPtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	strPtr := xunsafe.AsStringAddrPtr(ptr)
	if strPtr == nil || *strPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	marshallString(**strPtr, sb)
	return nil
}

func (i *stringPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddStringNull(xunsafe.AsStringAddrPtr(pointer))
}
