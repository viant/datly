package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type StringPtrMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewStringPtrMarshaller(dTag *DefaultTag) *StringPtrMarshaller {
	var zeroValue *string
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*string)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Quote(*zeroValue)
	}

	return &StringPtrMarshaller{
		dTag:         dTag,
		defaultValue: zeroString,
	}
}

func (i *StringPtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	strPtr := xunsafe.AsStringAddrPtr(ptr)
	if strPtr == nil || *strPtr == nil {
		sb.WriteString(i.defaultValue)
		return nil
	}

	marshallString(**strPtr, sb)
	return nil
}

func (i *StringPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddStringNull(xunsafe.AsStringAddrPtr(pointer))
}
