package registry

import (
	"context"
	"fmt"
	"github.com/viant/datly/codec"
	"github.com/viant/datly/router/marshal/csv"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox"
	"reflect"
	"unsafe"
)

type (
	CsvFactory string
	CSV        struct {
		marshaller *csv.Marshaller
		config     *csv.Config
		codec      *view.Codec
	}
)

func (c CsvFactory) Valuer() codec.Valuer {
	return c
}

func (c CsvFactory) Name() string {
	return CodecKeyCSV
}

func (c CsvFactory) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	return nil, fmt.Errorf("unexpected use Value on CsvFactory")
}

func (c *CSV) Valuer() codec.Valuer {
	return c
}

func (c CsvFactory) New(codec *view.Codec) (codec.Valuer, error) {
	aCsv := &CSV{
		codec: codec,
	}

	if err := aCsv.init(); err != nil {
		return nil, err
	}

	return aCsv, nil
}

func (c *CSV) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	rawString, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected value type, expected %T got %T", rawString, raw)
	}

	sliceType, cardinality := c.SliceType()
	dest := reflect.New(sliceType)

	if err := c.marshaller.Unmarshal([]byte(rawString), dest.Interface()); err != nil {
		return nil, err
	}

	toolbox.Dump(dest.Interface())

	if cardinality == view.Many {
		return dest.Elem().Interface(), nil
	}

	slicePtr := unsafe.Pointer(dest.Pointer())
	sliceLen := c.codec.Schema.Slice().Len(slicePtr)
	switch sliceLen {
	case 0:
		return nil, nil
	case 1:
		return c.codec.Schema.Slice().ValuePointerAt(slicePtr, 0), nil
	default:
		return nil, fmt.Errorf("unexpected number of records, wanted 1 got %v", sliceLen)
	}
}

func (c *CSV) SliceType() (reflect.Type, view.Cardinality) {
	if c.codec.Schema.Type().Kind() == reflect.Slice {
		return c.codec.Schema.Type(), view.Many
	}

	return c.codec.Schema.SliceType(), view.One
}

func (c *CSV) init() error {
	elemType := c.codec.Schema.Type()
	if elemType.Kind() == reflect.Slice {
		elemType = elemType.Elem()
	}

	var err error
	c.marshaller, err = csv.NewMarshaller(elemType, c.config)
	return err
}
