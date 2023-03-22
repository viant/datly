package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"strconv"
	"strings"
	"unicode"
	"unsafe"
)

type stringMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func newStringMarshaller(dTag *DefaultTag) *stringMarshaller {
	var zeroValue string
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(string)
	}

	zeroValue = strconv.Quote(zeroValue)

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

func (i *stringMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
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
