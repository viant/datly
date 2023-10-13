package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/structology/format"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type interfaceMarshaller struct {
	rType      reflect.Type
	config     *config.IOConfig
	path       string
	outputPath string
	tag        *format.Tag
	cache      *marshallersCache
	xType      *xunsafe.Type
	hasMethod  bool
}

func newInterfaceMarshaller(rType reflect.Type, config *config.IOConfig, path string, outputPath string, tag *format.Tag, cache *marshallersCache) (*interfaceMarshaller, error) {
	return &interfaceMarshaller{
		xType:      xunsafe.NewType(rType),
		rType:      rType,
		config:     config,
		path:       path,
		outputPath: outputPath,
		tag:        tag,
		cache:      cache,
		hasMethod:  rType.NumMethod() > 0,
	}, nil
}

func (i *interfaceMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	iface := i.AsInterface(pointer)
	return decoder.AddInterface(&iface)
}

func asInterface(xType *xunsafe.Type, pointer unsafe.Pointer) interface{} {
	if xType.Kind() == reflect.Interface {
		return xunsafe.AsInterface(pointer)
	}
	return xType.Interface(pointer)
}

func (i *interfaceMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	value := i.AsInterface(ptr)
	rType := reflect.TypeOf(value)

	marshaller, err := i.cache.loadMarshaller(rType, i.config, i.path, i.outputPath, i.tag)
	if err != nil {
		return err
	}

	pointer := AsPtr(value, rType)
	return marshaller.MarshallObject(pointer, sb)
}

func (i *interfaceMarshaller) AsInterface(ptr unsafe.Pointer) interface{} {
	if !i.hasMethod {
		return xunsafe.AsInterface(ptr)
	}

	return *(*interface {
		M()
	})(ptr)
}
