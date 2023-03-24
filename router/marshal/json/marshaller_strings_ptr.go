package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type stringPtrMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func newStringPtrMarshaller(dTag *DefaultTag) *stringPtrMarshaller {
	var zeroValue *string
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*string)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Quote(*zeroValue)
	}

	return &stringPtrMarshaller{
		dTag:         dTag,
		defaultValue: zeroString,
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
