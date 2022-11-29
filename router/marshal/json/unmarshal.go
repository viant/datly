package json

import (
	"bytes"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type (
	structDecoder struct {
		marshaller *Marshaller
		dest       interface{}
		ptr        unsafe.Pointer
		path       string
	}

	sliceDecoder struct {
		marshaller *Marshaller
		dest       interface{}
		ptr        unsafe.Pointer
		xSlice     *xunsafe.Slice
		appender   *xunsafe.Appender
		isStruct   bool
	}

	presenceUpdater struct {
		xField *xunsafe.Field
		fields map[string]*xunsafe.Field
	}
)

func (d *structDecoder) UnmarshalJSONObject(decoder *gojay.Decoder, fieldName string) error {
	marshaller, ok := d.marshaller.marshalerByName(fieldName)
	if !ok {
		return fmt.Errorf("not found field %v", fieldName)
	}

	ptr := marshaller.xField.Pointer(d.ptr)
	iface := marshaller.xType.Interface(xunsafe.RefPointer(ptr))

	err := decoder.AddInterface(&iface)
	if err != nil {
		return err
	}

	d.updatePresenceIfNeeded(marshaller)
	return nil
}

func (d *structDecoder) updatePresenceIfNeeded(marshaller *fieldMarshaller) {
	updater := marshaller.indexUpdater
	if updater == nil {
		return
	}

	xField := updater.fields[marshaller.fieldName]
	if xField == nil {
		return
	}

	ptr := updater.xField.ValuePointer(d.ptr)
	if ptr == nil {
		rValue := reflect.New(updater.xField.Type)
		ptr = xunsafe.ValuePointer(&rValue)
		fieldPtr := updater.xField.Pointer(d.ptr)
		*(*unsafe.Pointer)(fieldPtr) = ptr
	}

	xField.SetBool(ptr, true)
}

func (d *structDecoder) NKeys() int {
	return len(d.marshaller.marshallers)
}

func (j *Marshaller) Unmarshal(data []byte, dest interface{}) error {
	rValue := reflect.ValueOf(dest)
	if rValue.Kind() != reflect.Ptr {
		return fmt.Errorf("unsupported dest type, expected Ptr, got %T", dest)
	}

	decoder := gojay.NewDecoder(bytes.NewReader(data))

	elemType := rValue.Elem().Type()
	switch elemType.Kind() {
	case reflect.Struct:
		return decoder.Object(j.newStructDecoder("", dest))
	case reflect.Slice:
		return decoder.Array(j.newArrayDecoder(dest, elemType))
	}

	return decoder.Decode(dest)
}

func (j *Marshaller) newStructDecoder(path string, dest interface{}) gojay.UnmarshalerJSONObject {
	return &structDecoder{
		marshaller: j,
		dest:       dest,
		ptr:        xunsafe.AsPointer(dest),
		path:       path,
	}
}

func (j *Marshaller) newArrayDecoder(dest interface{}, elemType reflect.Type) gojay.UnmarshalerJSONArray {
	pointer := xunsafe.AsPointer(dest)
	slice := xunsafe.NewSlice(elemType)
	var isStruct bool
outer:
	for {
		switch elemType.Kind() {
		case reflect.Ptr, reflect.Slice, reflect.Map:
			elemType = elemType.Elem()
		default:
			isStruct = elemType.Kind() == reflect.Struct
			break outer
		}
	}
	return &sliceDecoder{
		marshaller: j,
		dest:       dest,
		ptr:        pointer,
		xSlice:     slice,
		appender:   slice.Appender(pointer),
		isStruct:   isStruct,
	}
}

func (s *sliceDecoder) UnmarshalJSONArray(decoder *gojay.Decoder) error {
	iface := s.appender.Add()
	if s.isStruct {
		return decoder.Object(s.marshaller.newStructDecoder("", iface))
	}

	return decoder.AddInterface(&iface)
}
