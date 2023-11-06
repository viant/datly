package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"unsafe"
)

type boolPtrMarshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newBoolPtrMarshaller(dTag *format.Tag) *boolPtrMarshaller {
	var zeroValue = "false"
	if dTag.IsNullable() {
		zeroValue = null
	}
	return &boolPtrMarshaller{
		dTag:      dTag,
		zeroValue: zeroValue,
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

func (i *boolPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddBool(xunsafe.AsBoolPtr(pointer))
}
