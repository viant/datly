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
	}, nil
}

func (i *InterfaceMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	iface := Interface(i.xType, pointer)
	return mainDecoder.AddInterface(&iface)
}

func Interface(xType *xunsafe.Type, pointer unsafe.Pointer) interface{} {
	if xType.Kind() == reflect.Interface {
		return xunsafe.AsInterface(pointer)
	}

	return xType.Interface(pointer)
}

func (i *InterfaceMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	value := Interface(GetXType(rType), ptr)
	marshaller, err := i.cache.LoadMarshaller(rType, i.config, i.path, i.outputPath, i.tag)
	if err != nil {
		return err
	}

	ptr = xunsafe.AsPointer(value)
	if rType.Kind() == reflect.Ptr {
		ptr = xunsafe.RefPointer(ptr)
	}

	return marshaller.MarshallObject(rType, ptr, sb)
}