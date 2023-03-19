package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type (
	decoder struct {
		ptr        unsafe.Pointer
		path       string
		xType      *xunsafe.Type
		marshaller *StructMarshaller
	}

	sliceDecoder struct {
		rType    reflect.Type
		ptr      unsafe.Pointer
		appender *xunsafe.Appender
		fn       unmarshallFieldFn
	}
)

func newSliceDecoder(rType reflect.Type, ptr unsafe.Pointer, xslice *xunsafe.Slice, unmarshaller unmarshallFieldFn) *sliceDecoder {
	return &sliceDecoder{
		rType:    rType,
		ptr:      ptr,
		appender: xslice.Appender(ptr),
		fn:       unmarshaller,
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

	marshaller, ok := d.marshaller.marshallerByName(fieldName)
	if !ok {
		return nil
	}

	fieldType := marshaller.xField.Type
	if fieldType.Kind() == reflect.Ptr {
		fieldPtr := marshaller.xField.Pointer(d.ptr)
		xunsafe.SafeDerefPointer(fieldPtr, fieldType)
	}
	fieldPtr := marshaller.xField.ValuePointer(d.ptr)
	if err := marshaller.marshaller.UnmarshallObject(marshaller.xField.Type, fieldPtr, decoder, nil); err != nil {
		return err
	}
	d.updatePresenceIfNeeded(marshaller)
	return nil
}

func (d *decoder) updatePresenceIfNeeded(marshaller *MarshallerWithField) {
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
	rType := reflect.TypeOf(dest).Elem()
	marshaller, err := j.cache.LoadMarshaller(rType, j.config, "", "", &DefaultTag{})
	if err != nil {
		return err
	}
	dec := gojay.NewDecoder(bytes.NewReader(data))
	err = marshaller.UnmarshallObject(rType, xunsafe.AsPointer(dest), dec, nil)
	return err
}
