package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"strings"
	"unicode"
	"unsafe"
)

type stringMarshaller struct {
	defaultValue string
	dTag         *format.Tag
	replacer     *strings.Replacer
}

func newStringMarshaller(dTag *format.Tag) *stringMarshaller {
	var zeroValue = `""`
	if dTag.IsNullable() {
		zeroValue = null
	}

	return &stringMarshaller{
		dTag:         dTag,
		defaultValue: zeroValue,
		replacer:     getReplacer(),
	}
}

func (i *stringMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	asString := xunsafe.AsString(ptr)
	if asString == "" {
		sb.WriteString(i.defaultValue)
		return nil
	}

	i.ensureReplacer()
	marshallString(asString, sb, i.replacer)
	return nil
}

func (i *stringMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	return decoder.AddString(xunsafe.AsStringPtr(pointer))
}

func (i *stringMarshaller) ensureReplacer() {
	if i.replacer == nil {
		i.replacer = getReplacer()
	}
}

func marshallString(asString string, sb *MarshallSession, replacer *strings.Replacer) {
	asString = strings.TrimFunc(asString, func(r rune) bool {
		return !unicode.IsGraphic(r)
	})

	sb.WriteByte('"')
	sb.WriteString(replacer.Replace(asString))
	sb.WriteByte('"')
}

func getReplacer() *strings.Replacer {
	return strings.NewReplacer(`\`, `\\`,
		`"`, `\"`,
		`/`, `\/`,
		"\b", `\b`,
		"\f", `\f`,
		"\n", `\n`,
		"\r", `\r`,
		"\t", `\t`)
}
