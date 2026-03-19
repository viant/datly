package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/tagly/format"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type customMarshaller struct {
	config     *config.IOConfig
	path       string
	outputPath string
	tag        *format.Tag
	cache      *marshallersCache
	marshaller marshaler
	valueType  *xunsafe.Type
	addrType   *xunsafe.Type
}

func newCustomUnmarshaller(rType reflect.Type, config *config.IOConfig, path string, outputPath string, tag *format.Tag, cache *marshallersCache) (marshaler, error) {
	// Build a base marshaller directly to avoid self-referencing deferred placeholders
	// when this function is invoked while the same type is under construction.
	marshaller, err := cache.pathCache(path).getMarshaller(rType, config, path, outputPath, tag, &cacheConfig{IgnoreCustomUnmarshaller: true})
	if err != nil {
		return nil, err
	}
	return newCustomUnmarshallerWithMarshaller(rType, config, path, outputPath, tag, cache, marshaller), nil
}

func newCustomUnmarshallerWithMarshaller(rType reflect.Type, config *config.IOConfig, path string, outputPath string, tag *format.Tag, cache *marshallersCache, marshaller marshaler) marshaler {
	return &customMarshaller{
		valueType:  getXType(rType),
		addrType:   getXType(reflect.PtrTo(rType)),
		config:     config,
		path:       path,
		outputPath: outputPath,
		tag:        tag,
		cache:      cache,
		marshaller: marshaller,
	}
}
func (c *customMarshaller) MarshallObject(ptr unsafe.Pointer, session *MarshallSession) error {
	return c.marshaller.MarshallObject(ptr, session)
}

func (c *customMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	value := c.valueType.Interface(pointer)
	asUnmarshaler, ok := value.(UnmarshalerInto)
	if ok {
		dst := c.addrType.Value(pointer)
		return asUnmarshaler.UnmarshalJSONWithOptions(dst, decoder, session.Options...)
	}

	return c.marshaller.UnmarshallObject(pointer, decoder, auxiliaryDecoder, session)
}
