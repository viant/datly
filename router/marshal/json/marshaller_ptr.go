package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type PtrMarshaller struct {
	rType     reflect.Type
	marshaler Marshaler
	isPtrLike bool
	xType     *xunsafe.Type
}

func NewPtrMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *Cache) (*PtrMarshaller, error) {
	elem := rType.Elem()
	marshaller, err := cache.LoadMarshaller(elem, config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	return &PtrMarshaller{
		xType:     GetXType(rType),
		isPtrLike: IsPtrLike(elem),
		rType:     rType,
		marshaler: marshaller,
	}, err
}

func (i *PtrMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters) error {
	if ptr != nil && i.isPtrLike {
		ptr = xunsafe.DerefPointer(ptr)
	}

	if ptr == nil {
		sb.Write(nullBytes)
		return nil
	}

	return i.marshaler.MarshallObject(rType, ptr, sb, filters)
}

func (i *PtrMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	if pointer == nil {
		return nil
	}

	if nullDecoder == nil {
		embeddedJSON := &gojay.EmbeddedJSON{}
		if err := mainDecoder.EmbeddedJSON(embeddedJSON); err != nil {
			return err
		}

		if bytes.Equal(*embeddedJSON, nullBytes) {
			return nil
		}

		nullDecoder = gojay.NewDecoder(bytes.NewReader(*embeddedJSON))
	}

	return i.marshaler.UnmarshallObject(rType, xunsafe.SafeDerefPointer(pointer, rType), nullDecoder, nullDecoder)
}

func IsPtrLike(rType reflect.Type) bool {
	switch rType.Kind() {
	case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Bool, reflect.Struct:
		return false
	default:
		return true
	}
}
