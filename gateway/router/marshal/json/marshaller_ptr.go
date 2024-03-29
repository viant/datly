package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type ptrMarshaller struct {
	rType       reflect.Type
	marshaler   marshaler
	xType       *xunsafe.Type
	isElemIface bool
}

func newPtrMarshaller(rType reflect.Type, config *config.IOConfig, path string, outputPath string, tag *format.Tag, cache *marshallersCache) (marshaler, error) {
	elem := rType.Elem()
	marshaller, err := cache.loadMarshaller(elem, config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	return &ptrMarshaller{
		xType:       getXType(elem),
		rType:       rType,
		marshaler:   marshaller,
		isElemIface: elem.Kind() == reflect.Interface,
	}, err
}

func (i *ptrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	if ptr == nil || (*unsafe.Pointer)(ptr) == nil || *(*unsafe.Pointer)(ptr) == nil {
		sb.Write(nullBytes)
		return nil
	}
	ptr = xunsafe.DerefPointer(ptr)
	if ptr == nil {
		sb.Write(nullBytes)
		return nil
	}

	return i.marshaler.MarshallObject(ptr, sb)
}

func (i *ptrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	if pointer == nil {
		return nil
	}

	if auxiliaryDecoder == nil {
		embeddedJSON := &gojay.EmbeddedJSON{}
		if err := decoder.EmbeddedJSON(embeddedJSON); err != nil {
			return err
		}

		if bytes.Equal(*embeddedJSON, nullBytes) {
			return nil
		}

		auxiliaryDecoder = gojay.NewDecoder(bytes.NewReader(*embeddedJSON))
	}

	return i.marshaler.UnmarshallObject(xunsafe.SafeDerefPointer(pointer, i.rType), auxiliaryDecoder, auxiliaryDecoder, session)
}
