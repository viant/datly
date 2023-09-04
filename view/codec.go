package view

import (
	"context"
	"fmt"
	codec2 "github.com/viant/datly/config/codec"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
)

const (
	rawFieldName    = "Raw"
	actualFieldName = "Actual"
)

type (
	columnsCodec struct {
		fields     []*xunsafe.Field
		selectors  []*structology.Selector
		unwrapper  *xunsafe.Field
		actualType reflect.Type
		columns    []*Column
	}
)

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
			Type: column.ColumnType(),
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
	stateType := structology.NewStateType(c.actualType, structology.WithCustomizedNames(func(name string, tag reflect.StructTag) []string {
		sqlxTag := io.ParseTag(tag.Get(option.TagSqlx))
		if sqlxTag.Column == "" {
			return []string{name}
		}
		return strings.Split(sqlxTag.Column, "|")
	}))

	for _, column := range columns {
		c.selectors = append(c.selectors, stateType.Lookup(actualFieldName+"."+column.Name))
	}
	return nil
}

func (c *columnsCodec) updateValue(ctx context.Context, value interface{}, record *codec2.ParentValue) error {
	asPtr := xunsafe.AsPointer(value)
	for i, column := range c.columns {
		fieldValue := c.fields[i].Value(asPtr)
		decoded, err := column.Codec.Transform(ctx, fieldValue, codec.WithOptions(record))
		if err != nil {
			return err
		}
		if err = c.selectors[i].SetValue(asPtr, decoded); err != nil {
			return err
		}
	}

	return nil
}
