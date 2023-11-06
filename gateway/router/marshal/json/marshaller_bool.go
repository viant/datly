package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"unsafe"
)

type boolMarshaller struct {
	zeroValue string
	dTag      *format.Tag
}

func newBoolMarshaller(dTag *format.Tag) *boolMarshaller {
	var zeroValue = "false"
	if dTag.IsNullable() {
		zeroValue = null
	}
	return &boolMarshaller{
		dTag:      dTag,
		zeroValue: zeroValue,
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

func (i *boolMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddBool(xunsafe.AsBoolPtr(pointer))
}

func marshallBool(b bool, sb *MarshallSession) {
	if b {
		sb.WriteString(`true`)
	} else {
		sb.WriteString(`false`)
	}
}
