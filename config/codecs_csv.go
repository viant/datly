package config

import (
	"context"
	"fmt"
	"github.com/viant/sqlx/io/load/reader/csv"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type (
	CsvFactory string
	CSV        struct {
		paramType  reflect.Type
		sliceType  *xunsafe.Slice
		marshaller *csv.Marshaller
		config     *csv.Config
		codec      *CodecConfig
	}
)

func (c *CSV) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return c.sliceType.Type, nil
}

func (c CsvFactory) Valuer() Valuer {
	panic(UnexpectedUseError("Valuer", c))
}

func (c CsvFactory) Name() string {
	return CodecKeyCSV
}

func (c CsvFactory) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	panic(UnexpectedUseError("Value", c))
}

func (c *CSV) Valuer() Valuer {
	return c
}

func (c CsvFactory) New(codec *CodecConfig, _ ...interface{}) (Valuer, error) {
	aCsv := &CSV{
		codec:     codec,
		paramType: codec.ParamType,
		sliceType: xunsafe.NewSlice(reflect.SliceOf(codec.ParamType)),
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

	dest := reflect.New(c.sliceType.Type)

	if err := c.marshaller.Unmarshal([]byte(rawString), dest.Interface()); err != nil {
		return nil, err
	}

	if c.paramType.Kind() == reflect.Slice {
		return dest.Elem().Interface(), nil
	}

	slicePtr := unsafe.Pointer(dest.Pointer())
	sliceLen := c.sliceType.Len(slicePtr)
	switch sliceLen {
	case 0:
		return nil, nil
	case 1:
		return c.sliceType.ValuePointerAt(slicePtr, 0), nil
	default:
		return nil, fmt.Errorf("unexpected number of records, wanted 1 got %v", sliceLen)
	}
}

func (c *CSV) init() error {
	elemType := c.paramType
	if elemType.Kind() == reflect.Slice {
		elemType = elemType.Elem()
	}

	var err error
	c.marshaller, err = csv.NewMarshaller(elemType, c.config)
	return err
}
