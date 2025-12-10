package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"strings"
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

func marshallString(asString string, sb *MarshallSession, _ *strings.Replacer) {
	// Fully JSON-escape the string, including control chars and JS line/paragraph separators.
	const hexDigits = "0123456789abcdef"
	sb.WriteByte('"')
	for i := 0; i < len(asString); i++ {
		c := asString[i]
		switch c {
		case '\\', '"':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case '/':
			sb.WriteByte('\\')
			sb.WriteByte('/')
		case '\b':
			sb.WriteString(`\\b`)
		case '\f':
			sb.WriteString(`\\f`)
		case '\n':
			sb.WriteString(`\\n`)
		case '\r':
			sb.WriteString(`\\r`)
		case '\t':
			sb.WriteString(`\\t`)
		default:
			// Escape other control characters < 0x20 as \u00XX
			if c < 0x20 {
				sb.WriteString(`\\u00`)
				sb.WriteByte(hexDigits[c>>4])
				sb.WriteByte(hexDigits[c&0x0F])
				continue
			}
			// Escape U+2028 and U+2029 to be safe for JS embed contexts
			if c == 0xE2 && i+2 < len(asString) {
				c1 := asString[i+1]
				c2 := asString[i+2]
				if c1 == 0x80 && (c2 == 0xA8 || c2 == 0xA9) {
					if c2 == 0xA8 {
						sb.WriteString(`\\u2028`)
					} else {
						sb.WriteString(`\\u2029`)
					}
					i += 2
					continue
				}
			}
			sb.WriteByte(c)
		}
	}
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
