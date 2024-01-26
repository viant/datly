package codec

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/converter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

const Encode = "Encode"

type (
	EncodeFactory struct {
	}

	Encoder struct {
		dstType   reflect.Type
		aSlice    *xunsafe.Slice
		separator string
		fields    []string
		_fields   []*xunsafe.Field
	}
)

func (e *EncodeFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if err := ValidateMinArgs(codecConfig, Encode, 3); err != nil {
		return nil, err
	}
	opts := NewOptions(codec.NewOptions(options))
	destType, err := types.LookupType(opts.LookupType, codecConfig.Args[0])
	if err != nil {
		return nil, err
	}

	if types.IsMulti(codecConfig.InputType) && !isMulti(destType) {
		destType = reflect.SliceOf(destType)
	}

	var aSlice *xunsafe.Slice
	if types.IsMulti(destType) {
		aSlice = xunsafe.NewSlice(destType, xunsafe.UseItemAddrOpt(false))
	}

	encoder := &Encoder{
		aSlice:    aSlice,
		dstType:   destType,
		separator: codecConfig.Args[1],
		fields:    codecConfig.Args[2:],
	}

	return encoder, encoder.init()
}

func (e *Encoder) init() error {
	fields, err := e.extractFields()
	if err != nil {
		return err
	}

	e._fields = fields
	return nil
}

func (e *Encoder) extractFields() ([]*xunsafe.Field, error) {
	elemType := types.Elem(e.dstType)
	fieldsTaken := map[int]bool{}
	result := make([]*xunsafe.Field, 0, len(e.fields))
	for _, field := range e.fields {
		aField, err := e.extractField(field, elemType, fieldsTaken)
		if err != nil {
			return nil, err
		}
		result = append(result, aField)
	}

	return result, nil
}

func (e *Encoder) extractField(field string, elemType reflect.Type, taken map[int]bool) (*xunsafe.Field, error) {
	aField := xunsafe.FieldByName(elemType, field)
	if aField == nil {
		return nil, fmt.Errorf("not found field %v at %v", field, elemType.String())
	}

	return aField, nil
}

func isMulti(destType reflect.Type) bool {
	return destType.Kind() == reflect.Slice || destType.Kind() == reflect.Array
}

func (e *Encoder) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return e.dstType, nil
}

func (e *Encoder) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {

	opts := codec.Options{}
	opts.Apply(options)

	if e.aSlice != nil {
		return e.encodeSlice(ctx, raw, opts)
	}

	return e.encodeStruct(ctx, raw, opts)
}

func (e *Encoder) encodeSlice(ctx context.Context, raw interface{}, options codec.Options) (interface{}, error) {
	strSlice, ok := raw.([]string)
	if !ok {
		return nil, UnexpectedValueType(strSlice, raw)
	}

	value := types.NewValue(e.dstType)
	appender := e.aSlice.Appender(xunsafe.AsPointer(value))

	for _, sliceItem := range strSlice {
		segments, err := e.split(sliceItem)
		if err != nil {
			return nil, err
		}
		item := appender.Add()
		if err = e.update(xunsafe.AsPointer(item), segments); err != nil {
			return nil, err
		}
	}
	return value, nil
}

func (e *Encoder) encodeStruct(ctx context.Context, raw interface{}, options codec.Options) (interface{}, error) {
	strValue, ok := asString(raw)
	if !ok {
		return nil, UnexpectedValueType(strValue, raw)
	}

	split, err := e.split(strValue)
	if err != nil {
		return nil, err
	}

	value := types.NewValue(e.dstType)
	if err = e.update(xunsafe.AsPointer(value), split); err != nil {
		return nil, err
	}

	return value, nil
}

func (e *Encoder) split(value string) ([]string, error) {
	split := strings.Split(value, e.separator)
	//if len(split) != len(e.fields) {
	//	return nil, fmt.Errorf("incorrect value format, expected %v values seperated with '%v' but got %v", len(e.fields), e.separator, len(split))
	//}

	return split, nil
}

func (e *Encoder) update(pointer unsafe.Pointer, split []string) error {
	for index, value := range split {
		aField := e._fields[index]
		converted, wasNil, err := converter.Convert(value, aField.Type, false, "")
		if err != nil {
			return err
		}

		if !wasNil {
			aField.SetValue(pointer, converted)
		}
	}

	return nil
}
