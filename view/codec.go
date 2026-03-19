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
	"unicode"
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
	cc := &columnsCodec{}
	if err := cc.init(viewType, withCodec); err != nil {
		return nil, err
	}
	return cc, nil
}

func (c *columnsCodec) init(viewType reflect.Type, columns []*Column) error {
	c.columns = columns

	// Build Raw holder: Col0..ColN with sqlx tags derived from column names.
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

	// Build outer type with:
	// - Raw embedded FIRST (no methods; keeps Col* promoted and offsets valid)
	// - Actual as a named (non-embedded) field to avoid reflect method-embedding panic
	c.actualType = reflect.StructOf([]reflect.StructField{
		{
			Name:      rawFieldName,
			Type:      rawType,
			Anonymous: true,
		},
		{
			Name:      actualFieldName,
			Type:      viewType,
			Anonymous: false,
		},
	})

	// Access Raw's Col{i} by promoted name on the OUTER type.
	c.fields = make([]*xunsafe.Field, len(columns))
	for i := 0; i < len(columns); i++ {
		colName := "Col" + strconv.Itoa(i)
		c.fields[i] = xunsafe.FieldByName(c.actualType, colName)
	}

	// Unwrapper points to Actual field (index 1).
	c.unwrapper = xunsafe.FieldByIndex(c.actualType, 1)

	// State type on OUTER for consistent selector addressing.
	stateType := structology.NewStateType(
		c.actualType,
		structology.WithCustomizedNames(func(name string, tag reflect.StructTag) []string {
			// Use sqlx tag for Raw Col fields to match incoming column names.
			sqlxTag := io.ParseTag(tag)
			if sqlxTag.Column == "" {
				return []string{name}
			}
			return strings.Split(sqlxTag.Column, "|")
		}),
	)

	// Build selectors into Actual.<Field>. Try exact column name first,
	// then snake_case -> UpperCamel fallback.
	for _, column := range columns {
		// Prefer explicit path into the Actual field.
		sel := stateType.Lookup(actualFieldName + "." + column.Name)
		if sel == nil {
			if goName := toUpperCamel(column.Name); goName != column.Name {
				sel = stateType.Lookup(actualFieldName + "." + goName)
			}
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
	// Point to the nested Actual field so structology selector matches that root.
	actualPtr := c.unwrapper.Pointer(asPtr)

	for i, column := range c.columns {
		if c.fields[i] == nil {
			return fmt.Errorf("codec raw field not found for column %q", column.Name)
		}
		if c.selectors[i] == nil {
			return fmt.Errorf("codec selector not found for column %q (tried Actual.%s and camel-cased variant)", column.Name, column.Name)
		}
		// Read raw DB value from promoted Col{i} field on the outer struct.
		fieldValue := c.fields[i].Value(asPtr)

		decoded, err := column.Codec.Transform(ctx, fieldValue, codec.WithOptions(record))
		if err != nil {
			return err
		}
		// Set decoded value into the nested Actual field using selector.
		if err = c.selectors[i].SetValue(actualPtr, decoded); err != nil {
			return err
		}
	}
	return nil
}

// toUpperCamel converts snake_case or mixed separators to UpperCamel (Go field style).
func toUpperCamel(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	capNext := true
	for _, r := range s {
		if r == '_' || r == '-' || r == ' ' || r == '.' {
			capNext = true
			continue
		}
		if capNext {
			b.WriteRune(unicode.ToUpper(r))
			capNext = false
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
