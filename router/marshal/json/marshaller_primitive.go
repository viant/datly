package json

import (
	"bytes"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unsafe"
)

type (
	marshallFieldFn   func(reflect.Type, unsafe.Pointer, *bytes.Buffer, *Filters) error
	unmarshallFieldFn func(rType reflect.Type, ptr unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error
	zeroCheckerFn     func(interface{}) bool

	PrimitiveMarshaller struct {
		zeroValue    interface{}
		zeroChecker  bool
		marshaller   marshallFieldFn
		unmarshaller unmarshallFieldFn
		tag          *DefaultTag
	}
)

func NewPrimitiveMarshaller(rType reflect.Type, defaultTag *DefaultTag) (*PrimitiveMarshaller, error) {
	marshaller, unmarshaller, ok := getPrimitiveMarshallers(rType, defaultTag)
	if !ok {
		return nil, fmt.Errorf("expected %v to be primitive but was %v", rType.String(), rType.Kind())
	}

	return &PrimitiveMarshaller{
		tag:          defaultTag,
		zeroValue:    reflect.Zero(rType),
		marshaller:   marshaller,
		unmarshaller: unmarshaller,
	}, nil
}

func (p *PrimitiveMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
	return p.unmarshaller(rType, pointer, mainDecoder, nullDecoder)
}

func (p *PrimitiveMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters, opts ...Option) error {
	return p.marshaller(rType, ptr, sb, filters)
}

func getPrimitiveMarshallers(rType reflect.Type, defaultTag *DefaultTag) (marshallFieldFn, unmarshallFieldFn, bool) {
	switch rType.Kind() {
	case reflect.Int:
		marshaller, unmarshaller := intMarshaller(defaultTag)
		return marshaller, unmarshaller, true

	case reflect.Int8:
		marshaller, unmarshaller := int8Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Int16:
		marshaller, unmarshaller := int16Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Int32:
		marshaller, unmarshaller := int32Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Int64:
		marshaller, unmarshaller := int64Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Uint:
		marshaller, unmarshaller := uintMarshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Uint8:
		marshaller, unmarshaller := uint8Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Uint16:
		marshaller, unmarshaller := uint16Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Uint32:
		marshaller, unmarshaller := uint32Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Uint64:
		marshaller, unmarshaller := uint64Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Bool:
		marshaller, unmarshaller := boolMarshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.String:
		marshaller, unmarshaller := stringMarshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Float64:
		marshaller, unmarshaller := float64Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	case reflect.Float32:
		marshaller, unmarshaller := float32Marshaller(defaultTag)
		return marshaller, unmarshaller, true
	}

	if rType == xreflect.TimeType {

	}

	return nil, nil, false
}

func float32Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendFloat(sb, float64(xunsafe.AsFloat32(ptr)), false, tag)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddFloat32((*float32)(pointer))
		}
}

func float64Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendFloat(sb, xunsafe.AsFloat64(ptr), false, tag)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddFloat64(xunsafe.AsFloat64Ptr(pointer))
		}
}

func appendFloat(sb *bytes.Buffer, f float64, wasNull bool, tag *DefaultTag) error {
	if wasNull {
		sb.WriteString(null)
		return nil
	}

	if f == 0 && tag._value != nil {
		sb.WriteString(strconv.FormatFloat(tag._value.(float64), 'f', -1, 64))
		return nil
	}

	if wasNull {
		sb.WriteString("0")
		return nil
	}

	sb.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
	return nil
}

func stringMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			aString := xunsafe.AsString(ptr)
			if aString == "" && tag._value != nil {
				aString = tag._value.(string)
			}

			marshallString(sb, aString)
			return nil
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddString(xunsafe.AsStringPtr(pointer))
		}
}

func bytesMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			aString := xunsafe.AsString(ptr)
			if aString == "" && tag._value != nil {
				aString = tag._value.(string)
			}

			marshallString(sb, aString)
			return nil
		}, func(r reflect.Type, pointer unsafe.Pointer, g *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return g.DecodeArray(&BytesSlice{b: (*[]byte)(pointer)})
		}
}

func marshallString(sb *bytes.Buffer, asString string) {
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

func boolMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			marshallBool(xunsafe.AsBool(ptr), sb)
			return nil
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddBool(xunsafe.AsBoolPtr(pointer))
		}
}

func marshallBool(b bool, sb *bytes.Buffer) {
	if b {
		sb.WriteString(`true`)
	} else {
		sb.WriteString(`false`)
	}
}

func uint64Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint64(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddUint64(xunsafe.AsUint64Ptr(pointer))
		}
}

func uint32Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint32(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddUint32((*uint32)(pointer))
		}
}

func uint16Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint16(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddUint16((*uint16)(pointer))
		}
}

func uint8Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint8(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddUint8((*uint8)(pointer))
		}
}

func uintMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddUint64((*uint64)((pointer)))
		}
}

func int64Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsInt64(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddInt64((*int64)((pointer)))
		}
}

func int32Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsInt32(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddInt32((*int32)((pointer)))
		}
}

func int16Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsInt16(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddInt16((*int16)((pointer)))
		}
}

func int8Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsInt8(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddInt8((*int8)((pointer)))
		}
}

func intMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(xunsafe.AsInt(ptr), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error {
			return decoder.AddInt(xunsafe.AsIntPtr((pointer)))
		}
}

func appendInt(value int, wasNull bool, aTag *DefaultTag, sb *bytes.Buffer) error {
	if wasNull {
		sb.WriteString(null)
		return nil
	}

	if aTag._value != nil && value == 0 {
		sb.WriteString(strconv.Itoa(aTag._value.(int)))
		return nil
	}

	sb.WriteString(strconv.Itoa(value))
	return nil
}
