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
		fields      []*xunsafe.Field        // promoted Raw.Col{i} fields on the OUTER wrapper
		selectors   []*structology.Selector // selectors built against the Actual type
		unwrapper   *xunsafe.Field          // points to OUTER.Actual
		actualType  reflect.Type            // OUTER wrapper reflect type
		actualState *structology.StateType  // structology state built for the Actual type
		columns     []*Column
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

	// Build Raw holder with promoted Col0..ColN having sqlx tags that match result set column names.
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

	// OUTER wrapper:
	// - Raw embedded FIRST (no methods) so Col{i} remain promoted and offsets valid.
	// - Actual as a named (non-embedded) field to avoid reflect panic for method-bearing types.
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

	// Access Raw.Col{i} by promoted name on the OUTER wrapper.
	c.fields = make([]*xunsafe.Field, len(columns))
	for i := 0; i < len(columns); i++ {
		colName := "Col" + strconv.Itoa(i)
		c.fields[i] = xunsafe.FieldByName(c.actualType, colName)
	}

	// OUTER.Actual field (index 1)
	c.unwrapper = xunsafe.FieldByIndex(c.actualType, 1)

	// Build structology state for the Actual type so its field tags (e.g., sqlx:"...") are honored.
	c.actualState = structology.NewStateType(
		viewType,
		structology.WithCustomizedNames(func(name string, tag reflect.StructTag) []string {
			// Respect sqlx tag on Actual fields (can be "COL" or "COL|ALT")
			sqlxTag := io.ParseTag(tag)
			if sqlxTag.Column == "" {
				return []string{name}
			}
			return strings.Split(sqlxTag.Column, "|")
		}),
	)

	// Build selectors directly against the Actual state, trying several name variants.
	for _, column := range columns {
		var sel *structology.Selector
		candidates := []string{
			column.Name,                  // exact alias (e.g., AD_ORDERS_DATA_INDEX)
			strings.ToLower(column.Name), // lowercase alias for tags using lowercase
			toUpperCamel(column.Name),    // Go field name (AdOrdersDataIndex)
		}
		for _, cand := range candidates {
			if cand == "" {
				continue
			}
			sel = c.actualState.Lookup(cand)
			if sel != nil {
				break
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
	outerPtr := xunsafe.AsPointer(value)
	actualPtr := c.unwrapper.Pointer(outerPtr) // pointer to the Actual value

	for i, column := range c.columns {
		if c.fields[i] == nil {
			return fmt.Errorf("codec raw field not found for column %q", column.Name)
		}
		if c.selectors[i] == nil {
			return fmt.Errorf("codec selector not found for column %q (tried sqlx tag, lowercase tag, and camel-cased name)", column.Name)
		}
		rawFieldValue := c.fields[i].Value(outerPtr)

		decoded, err := column.Codec.Transform(ctx, rawFieldValue, codec.WithOptions(record))
		if err != nil {
			return err
		}
		if err = c.selectors[i].SetValue(actualPtr, decoded); err != nil {
			return err
		}
	}
	return nil
}

// toUpperCamel converts snake/space/hyphen/dot separated names to UpperCamel (Go field style).
// "AD_ORDERS_DATA_INDEX" -> "AdOrdersDataIndex"
func toUpperCamel(s string) string {
	if s == "" {
		return s
	}
	// Normalize separators to space
	normalized := make([]rune, 0, len(s))
	for _, r := range s {
		switch r {
		case '_', '-', ' ', '.':
			normalized = append(normalized, ' ')
		default:
			normalized = append(normalized, r)
		}
	}
	parts := strings.Fields(strings.ToLower(string(normalized)))
	if len(parts) == 0 {
		return ""
	}
	var b strings.Builder
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		for i, r := range p {
			if i == 0 {
				b.WriteRune(unicode.ToUpper(r))
			} else {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}
