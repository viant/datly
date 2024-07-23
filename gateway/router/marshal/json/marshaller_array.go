package json

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/tagly/format"
	"reflect"
	"strconv"
	"unsafe"
)

type (
	arrayMarshaller struct {
		elemType   reflect.Type
		array      reflect.Type
		marshaller marshaler
		path       string
	}
)

func newArrayMarshaller(rType reflect.Type, config *config.IOConfig, path string, outputPath string, tag *format.Tag, cache *marshallersCache) (marshaler, error) {
	elemType := rType.Elem()
	return &arrayMarshaller{
		path:     path,
		elemType: elemType,
		array:    rType,
	}, nil
}

func (s *arrayMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	if skipNull(decoder) {
		return nil
	}
	return fmt.Errorf("unsupported array unmarshalling")
}

func (s *arrayMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	arrayPtr := reflect.NewAt(s.array, ptr)
	array := arrayPtr.Elem()
	if array.Len() == 0 {
		sb.WriteString("[]")
		return nil
	}
	sb.WriteByte('[')
	for i := 0; i < array.Len(); i++ {
		if i != 0 {
			sb.WriteByte(',')
		}
		switch s.elemType.Kind() {
		case reflect.Bool:
			item := array.Index(i).Bool()
			sb.WriteString(strconv.FormatBool(item))
		default:
			return fmt.Errorf("unsupported array marshalling")
		}
	}
	sb.WriteByte(']')
	return nil
}
