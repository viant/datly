package view

import (
	"context"
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
)

const (
	rawFieldName    = "Raw"
	actualFieldName = "Actual"
)

type columnsCodec struct {
	fields    []*xunsafe.Field
	accessors []*Accessor

	unwrapper  *xunsafe.Field
	actualType reflect.Type
	columns    []*Column
}

func newColumnsCodec(viewType reflect.Type, columns []*Column) (*columnsCodec, error) {
	var withCodec []*Column
	for i, column := range columns {
		if column.Codec != nil {
			withCodec = append(withCodec, columns[i])
		}
	}

	if len(withCodec) == 0 {
		return nil, nil
	}

	codec := &columnsCodec{}

	if err := codec.init(viewType, withCodec); err != nil {
		return nil, err
	}

	return codec, nil
}

func (c *columnsCodec) init(viewType reflect.Type, columns []*Column) error {
	c.columns = columns
	codecStructFields := make([]reflect.StructField, len(columns))
	for i, column := range columns {
		codecStructFields[i] = reflect.StructField{
			Name: "Col" + strconv.Itoa(i),
			Type: column.rType,
			Tag:  reflect.StructTag(fmt.Sprintf(`sqlx:"name=%v"`, column.Name)),
		}
	}

	rawType := reflect.StructOf(codecStructFields)
	c.actualType = reflect.StructOf([]reflect.StructField{
		{
			Name:      rawFieldName,
			Type:      rawType,
			Anonymous: true,
		},
		{
			Name:      actualFieldName,
			Type:      viewType,
			Anonymous: true,
		},
	})

	c.fields = make([]*xunsafe.Field, len(columns))
	for i := 0; i < len(columns); i++ {
		c.fields[i] = xunsafe.FieldByIndex(rawType, i)
	}

	c.unwrapper = xunsafe.FieldByIndex(c.actualType, 1)
	accessors := Accessors{
		namer: &SqlxNamer{},
		index: map[string]int{},
	}
	accessors.Init(c.actualType)

	c.accessors = make([]*Accessor, len(columns))
	for i, column := range columns {
		c.accessors[i], _ = accessors.AccessorByName(actualFieldName + "." + column.Name)
	}

	return nil
}

func (c *columnsCodec) updateValue(ctx context.Context, value interface{}) error {
	asPtr := xunsafe.AsPointer(value)
	for i, column := range c.columns {
		fieldValue := c.fields[i].Value(asPtr)
		decoded, err := column.Codec._codecFn(ctx, fieldValue)
		if err != nil {
			return err
		}

		c.accessors[i].set(asPtr, decoded)
	}

	return nil
}
