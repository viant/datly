package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type boolPtrMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newBoolPtrMarshaller(dTag *DefaultTag) *boolPtrMarshaller {
	var zeroValue *bool
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(*bool)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.FormatBool(*zeroValue)
	}

	return &boolPtrMarshaller{
		dTag:      dTag,
		zeroValue: zeroString,
	}
}

func (i *boolPtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asBoolPtr := xunsafe.AsBoolAddrPtr(ptr)
	if asBoolPtr == nil || *asBoolPtr == nil {
		sb.WriteString(i.zeroValue)
		return nil
	}

	marshallBool(**asBoolPtr, sb)
	return nil
}

func (i *boolPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddBool(xunsafe.AsBoolPtr(pointer))
}
