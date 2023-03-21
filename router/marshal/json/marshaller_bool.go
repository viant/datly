package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type BoolMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func NewBoolMarshaller(dTag *DefaultTag) *BoolMarshaller {
	var zeroValue bool
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(bool)
	}

	return &BoolMarshaller{
		dTag:      dTag,
		zeroValue: strconv.FormatBool(zeroValue),
	}
}

func (i *BoolMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	aBool := xunsafe.AsBoolPtr(ptr)
	if aBool == nil || !*aBool {
		sb.WriteString(i.zeroValue)
		return nil
	}

	marshallBool(*aBool, sb)
	return nil
}

func (i *BoolMarshaller) UnmarshallObject(pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddBool(xunsafe.AsBoolPtr(pointer))
}

func marshallBool(b bool, sb *Session) {
	if b {
		sb.WriteString(`true`)
	} else {
		sb.WriteString(`false`)
	}
}
