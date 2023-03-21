package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type InterfaceMarshaller struct {
	rType      reflect.Type
	config     marshal.Default
	path       string
	outputPath string
	tag        *DefaultTag
	cache      *Cache
	xType      *xunsafe.Type
	hasMethod  bool
}

func NewInterfaceMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *Cache) (*InterfaceMarshaller, error) {
	return &InterfaceMarshaller{
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

func (i *InterfaceMarshaller) UnmarshallObject(pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	iface := Interface(i.xType, pointer)
	return mainDecoder.AddInterface(&iface)
}

func Interface(xType *xunsafe.Type, pointer unsafe.Pointer) interface{} {
	if xType.Kind() == reflect.Interface {
		return xunsafe.AsInterface(pointer)
	}

	return xType.Interface(pointer)
}

func (i *InterfaceMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	value := i.AsInterface(ptr)
	rType := reflect.TypeOf(value)

	marshaller, err := i.cache.LoadMarshaller(rType, i.config, i.path, i.outputPath, i.tag)
	if err != nil {
		return err
	}

	pointer := AsPtr(value, rType)
	return marshaller.MarshallObject(pointer, sb)
}

func (i *InterfaceMarshaller) AsInterface(ptr unsafe.Pointer) interface{} {
	if !i.hasMethod {
		return xunsafe.AsInterface(ptr)
	}

	return *(*interface {
		M()
	})(ptr)
}
