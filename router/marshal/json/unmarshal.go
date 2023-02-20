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
	decoder struct {
		marshaller *Marshaller
		dest       interface{}
		ptr        unsafe.Pointer
		path       string
		xType      *xunsafe.Type
	}

	sliceDecoder struct {
		rType    reflect.Type
		ptr      unsafe.Pointer
		appender *xunsafe.Appender
		fn       unmarshallFieldFn
		xType    *xunsafe.Type
		isPtr    bool
	}

	presenceUpdater struct {
		xField *xunsafe.Field
		fields map[string]*xunsafe.Field
	}
)

func newSliceDecoder(rType reflect.Type, ptr unsafe.Pointer, xslice *xunsafe.Slice, unmarshaller unmarshallFieldFn, xType *xunsafe.Type) *sliceDecoder {
	return &sliceDecoder{
		rType:    rType,
		ptr:      ptr,
		appender: xslice.Appender(ptr),
		fn:       unmarshaller,
		xType:    xType,
		isPtr:    xType.Kind() == reflect.Ptr,
	}
}

func (s *sliceDecoder) UnmarshalJSONArray(d *gojay.Decoder) error {
	add := s.appender.Add()
	return s.fn(s.rType, xunsafe.AsPointer(add), d, nil)
}

type Fieldx struct {
	hasField   *xunsafe.Field
	valueField *xunsafe.Field
	decoder    func(decoder *gojay.Decoder) (interface{}, error)
}

func (d *decoder) UnmarshalJSONObject(decoder *gojay.Decoder, fieldName string) error {
	marshaller, ok := d.marshaller.marshalerByName(fieldName)
	if !ok {
		return nil
	}

	if err := marshaller.unmarshal(marshaller.xField.Type, marshaller.xField.Pointer(d.ptr), decoder, nil); err != nil {
		return err
	}

	d.updatePresenceIfNeeded(marshaller)
	return nil
}

func (d *decoder) updatePresenceIfNeeded(marshaller *fieldMarshaller) {
	updater := marshaller.indexUpdater
	if updater == nil {
		return
	}

	xField := updater.fields[marshaller.fieldName]
	if xField == nil {
		return
	}

	ptr := updater.xField.ValuePointer(d.ptr)
	xField.SetBool(ptr, true)
}

func (d *decoder) NKeys() int {
	return len(d.marshaller.marshallers)
}

func (j *Marshaller) Unmarshal(data []byte, dest interface{}) error {
	err := j.unmarshal(data, dest)
	return err
}

func (j *Marshaller) unmarshal(data []byte, dest interface{}) error {
	rValue := reflect.ValueOf(dest)
	if rValue.Kind() != reflect.Ptr {
		return fmt.Errorf("unsupported dest type, expected Ptr, got %T", dest)
	}

	d := gojay.NewDecoder(bytes.NewReader(data))

	elemType := rValue.Elem().Type()
	switch elemType.Kind() {
	case reflect.Struct:
		return j.unmarshalElem(elemType, xunsafe.AsPointer(dest), d, nil)
	case reflect.Slice:
		return j.unmarshalArr(elemType, xunsafe.AsPointer(dest), d, nil)
	}

	return d.Decode(dest)
}

func (j *Marshaller) newStructDecoder(path string, dest interface{}, xType *xunsafe.Type) gojay.UnmarshalerJSONObject {
	destPtr := xunsafe.AsPointer(dest)

	if j.indexUpdater != nil {
		indexPtr := j.indexUpdater.xField.ValuePointer(destPtr)
		if indexPtr == nil {
			var rValue reflect.Value
			if j.indexUpdater.xField.Type.Kind() == reflect.Ptr {
				rValue = reflect.New(j.indexUpdater.xField.Type.Elem())
			} else {
				rValue = reflect.New(j.indexUpdater.xField.Type)
			}

			iface := rValue.Interface()
			j.indexUpdater.xField.SetValue(destPtr, iface)
		}
	}

	return &decoder{
		marshaller: j,
		xType:      xType,
		dest:       dest,
		ptr:        destPtr,
		path:       path,
	}
}
