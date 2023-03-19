package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unsafe"
)

type StringMarshaller struct {
	defaultValue string
	dTag         *DefaultTag
}

func NewStringMarshaller(dTag *DefaultTag) *StringMarshaller {
	var zeroValue string
	if dTag._value != nil {
		zeroValue, _ = dTag._value.(string)
	}

	zeroValue = strconv.Quote(zeroValue)

	return &StringMarshaller{
		dTag:         dTag,
		defaultValue: zeroValue,
	}
}

func (i *StringMarshaller) MarshallObject(_ reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	asString := xunsafe.AsString(ptr)
	if asString == "" {
		sb.WriteString(i.defaultValue)
		return nil
	}

	marshallString(asString, sb)
	return nil
}

func (i *StringMarshaller) UnmarshallObject(_ reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	return mainDecoder.AddString(xunsafe.AsStringPtr(pointer))
}

func marshallString(asString string, sb *Session) {
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
