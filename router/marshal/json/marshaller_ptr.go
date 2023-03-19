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
	rType       reflect.Type
	marshaler   Marshaler
	shouldDeref bool
	xType       *xunsafe.Type
}

func NewPtrMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *Cache) (Marshaler, error) {
	elem := rType.Elem()
	marshaller, ok := cache.getPredefinedPtrMarshaller(elem, config, path, outputPath, tag, cache)
	if ok {
		return marshaller, nil
	}

	marshaller, err := cache.LoadMarshaller(elem, config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	return &PtrMarshaller{
		xType:       GetXType(elem),
		shouldDeref: elem.Kind() == reflect.Ptr,
		rType:       rType,
		marshaler:   marshaller,
	}, err
}

func (i *PtrMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	ptr = xunsafe.DerefPointer(ptr)

	if ptr == nil {
		sb.Write(nullBytes)
		return nil
	}

	return i.marshaler.MarshallObject(rType, ptr, sb)
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
