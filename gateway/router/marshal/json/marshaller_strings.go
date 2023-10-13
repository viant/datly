package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/structology/format"
	"github.com/viant/xunsafe"
	"strings"
	"unicode"
	"unsafe"
)

type stringMarshaller struct {
	defaultValue string
	dTag         *format.Tag
}

func newStringMarshaller(dTag *format.Tag) *stringMarshaller {
	var zeroValue = `""`
	if dTag.IsNullable() {
		zeroValue = null
	}
	return &stringMarshaller{
		dTag:         dTag,
		defaultValue: zeroValue,
	}
}

func (i *stringMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asString := xunsafe.AsString(ptr)
	if asString == "" {
		sb.WriteString(i.defaultValue)
		return nil
	}

	marshallString(asString, sb)
	return nil
}

func (i *stringMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddString(xunsafe.AsStringPtr(pointer))
}

func marshallString(asString string, sb *MarshallSession) {
	asString = strings.TrimFunc(asString, func(r rune) bool {
		return !unicode.IsGraphic(r)
	})

	sb.WriteByte('"')
	if strings.Contains(asString, `"`) {
		sb.WriteString(strings.ReplaceAll(strings.ReplaceAll(asString, `\`, `\\`), `"`, `\"`))
	} else {
		sb.WriteString(asString)
	}

	sb.WriteByte('"')
}
