package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type BoolPtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewBoolPtrMarshaller(dTag *DefaultTag) *BoolPtrMarshaller {
	var zeroValue *bool
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*bool)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.FormatBool(*zeroValue)
	}

	return &BoolPtrMarshaller{
		dTag:      dTag,
		zeroValue: zeroString,
	}
}

func (i *BoolPtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	asBoolPtr := xunsafe.AsBoolAddrPtr(ptr)
	if asBoolPtr == nil || *asBoolPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	marshallBool(**asBoolPtr, sb)
	return nil
}

func (i *BoolPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddBool(xunsafe.AsBoolPtr(pointer))
}
