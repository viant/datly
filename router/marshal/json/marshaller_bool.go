package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"unsafe"
)

type boolMarshaller struct {
	zeroValue string
	dTag      *DefaultTag
}

func newBoolMarshaller(dTag *DefaultTag) *boolMarshaller {
	var zeroValue bool
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(bool)
	}

	return &boolMarshaller{
		dTag:      dTag,
		zeroValue: strconv.FormatBool(zeroValue),
	}
}

func (i *boolMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	aBool := xunsafe.AsBoolPtr(ptr)
	if aBool == nil || !*aBool {
		sb.WriteString(i.zeroValue)
		return nil
	}

	marshallBool(*aBool, sb)
	return nil
}

func (i *boolMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	return decoder.AddBool(xunsafe.AsBoolPtr(pointer))
}

func marshallBool(b bool, sb *MarshallSession) {
	if b {
		sb.WriteString(`true`)
	} else {
		sb.WriteString(`false`)
	}
}
