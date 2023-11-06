package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"strings"
	"unsafe"
)

type stringPtrMarshaller struct {
	defaultValue string
	dTag         *format.Tag
	replacer     *strings.Replacer
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

	i.ensureReplacer()
	marshallString(**strPtr, sb, i.replacer)
	return nil
}

func (i *stringPtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddStringNull(xunsafe.AsStringAddrPtr(pointer))
}

func (i *stringPtrMarshaller) ensureReplacer() {
	if i.replacer == nil {
		i.replacer = getReplacer()
	}
}
