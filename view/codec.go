package view

import (
	"context"
	"fmt"
	codec2 "github.com/viant/datly/view/extension/codec"
	"github.com/viant/sqlx/io"
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
		scanType := columnDatabaseScanType(column)
		codecStructFields[i] = reflect.StructField{
			Name: "Col" + strconv.Itoa(i),
			Type: scanType,
			Tag:  reflect.StructTag(fmt.Sprintf(`sqlx:"%v"`, column.Name)),
		}
	}

	rawType := reflect.StructOf(codecStructFields)

	// Embed Actual first (anonymous) to satisfy reflect rules when the type has methods.
	// Keep Raw embedded (anonymous) as well; we will resolve its Col* fields by promoted names.
	c.actualType = reflect.StructOf([]reflect.StructField{
		{
			Name:      actualFieldName,
			Type:      viewType,
			Anonymous: true,
		},
		{
			Name:      rawFieldName,
			Type:      rawType,
			Anonymous: true,
		},
	})

	// Resolve Raw's Col{i} fields by promoted name on the OUTER type (since Raw is embedded).
	c.fields = make([]*xunsafe.Field, len(columns))
	for i := 0; i < len(columns); i++ {
		colName := "Col" + strconv.Itoa(i)
		c.fields[i] = xunsafe.FieldByName(c.actualType, colName)
	}

	// Actual is at index 0 now.
	c.unwrapper = xunsafe.FieldByIndex(c.actualType, 0)

	stateType := structology.NewStateType(c.actualType, structology.WithCustomizedNames(func(name string, tag reflect.StructTag) []string {
		sqlxTag := io.ParseTag(tag)
		if sqlxTag.Column == "" {
			return []string{name}
		}
		return strings.Split(sqlxTag.Column, "|")
	}))

	for _, column := range columns {
		// Prefer explicit "Actual.<Field>" selector; fall back to promoted "<Field>" if applicable.
		sel := stateType.Lookup(actualFieldName + "." + column.Name)
		if sel == nil {
			sel = stateType.Lookup(column.Name)
		}
		c.selectors = append(c.selectors, sel)
	}
	return nil
}

func columnDatabaseScanType(column *Column) reflect.Type {
	if column == nil {
		return reflect.TypeOf([]byte{})
	}
	if column.Codec != nil && column.Codec.Name == codec2.JSON {
		return reflect.TypeOf([]byte{})
	}
	return column.ColumnType()
}

func (c *columnsCodec) updateValue(ctx context.Context, value interface{}, record *codec2.ParentValue) error {
	asPtr := xunsafe.AsPointer(value)
	for i, column := range c.columns {
		if c.fields[i] == nil {
			return fmt.Errorf("codec raw field not found for column %q", column.Name)
		}
		if c.selectors[i] == nil {
			return fmt.Errorf("codec selector not found for column %q", column.Name)
		}
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
