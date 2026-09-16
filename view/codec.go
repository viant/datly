package view

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	codec2 "github.com/viant/datly/view/extension/codec"
	"github.com/viant/sqlx/io"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
)

const (
	rawFieldName    = "Raw"
	shadowFieldName = "Shadow"
	actualFieldName = "Actual"
)

type (
	// columnsCodec builds a wrapper type:
	//   - Raw:    embedded, holds promoted Col{i} with sqlx tags for DB scan of codec sources
	//   - Shadow: embedded, mirrors Actual's exported fields+tags so sqlx can scan non-codec cols
	//   - Actual: named (non-embedded) model type to avoid reflect panic when it has methods
	columnsCodec struct {
		// Promoted Raw.Col{i} fields on OUTER wrapper
		fields []*xunsafe.Field

		// structology selectors resolved on OUTER with path "Actual.<...>" (codec targets)
		selectors []*structology.Selector

		// OUTER.Shadow (idx=1) and OUTER.Actual (idx=2)
		shadowField     *xunsafe.Field
		unwrapperActual *xunsafe.Field

		// Back-compat alias so view.go references to v._codec.unwrapper still compile
		unwrapper *xunsafe.Field

		// OUTER wrapper reflect type
		actualType reflect.Type

		// Columns that have codecs
		columns []*Column

		// Actual pointer handling
		actualIsPtr   bool
		actualElemTyp reflect.Type

		// Shadow -> Actual copy (safe reflect-based)
		shadowFieldNames []string // exported field names mirrored in Shadow and Actual
	}
)

func newColumnsCodec(viewType reflect.Type, columns []*Column) (*columnsCodec, error) {
	var withCodec []*Column
	for i, column := range columns {
		if column != nil && column.Codec != nil {
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

	// Build Raw holder: Col0..ColN with sqlx tags that match result set column names.
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

	// Determine Actual element type and build Shadow (exported fields only, preserving tags).
	c.actualIsPtr = viewType.Kind() == reflect.Ptr
	c.actualElemTyp = viewType
	if c.actualIsPtr {
		c.actualElemTyp = viewType.Elem()
	}
	shadowFields := make([]reflect.StructField, 0, c.actualElemTyp.NumField())
	c.shadowFieldNames = make([]string, 0, c.actualElemTyp.NumField())
	for i := 0; i < c.actualElemTyp.NumField(); i++ {
		f := c.actualElemTyp.Field(i)
		// Skip unexported fields
		if f.PkgPath != "" {
			continue
		}
		shadowFields = append(shadowFields, reflect.StructField{
			Name: f.Name,
			Type: f.Type,
			Tag:  f.Tag, // preserve sqlx tags for scanning
		})
		c.shadowFieldNames = append(c.shadowFieldNames, f.Name)
	}
	shadowType := reflect.StructOf(shadowFields)

	// OUTER wrapper layout:
	//  - Raw    (embedded, first)
	//  - Shadow (embedded, second)
	//  - Actual (named, third)
	c.actualType = reflect.StructOf([]reflect.StructField{
		{
			Name:      rawFieldName,
			Type:      rawType,
			Anonymous: true,
		},
		{
			Name:      shadowFieldName,
			Type:      shadowType,
			Anonymous: true,
		},
		{
			Name:      actualFieldName,
			Type:      viewType,
			Anonymous: false, // non-embedded to avoid reflect panic if model has methods
		},
	})

	// Promoted Raw.Col{i} on OUTER
	c.fields = make([]*xunsafe.Field, len(columns))
	for i := 0; i < len(columns); i++ {
		colName := "Col" + strconv.Itoa(i)
		c.fields[i] = xunsafe.FieldByName(c.actualType, colName)
	}

	// Shadow and Actual fields on OUTER
	// Indexes: 0=Raw, 1=Shadow, 2=Actual
	c.shadowField = xunsafe.FieldByIndex(c.actualType, 1)
	c.unwrapperActual = xunsafe.FieldByIndex(c.actualType, 2)
	// Back-compat alias so view.go can still use v._codec.unwrapper
	c.unwrapper = c.unwrapperActual

	// Build structology state for OUTER wrapper (honor sqlx tags anywhere)
	stateType := structology.NewStateType(
		c.actualType,
		structology.WithCustomizedNames(func(name string, tag reflect.StructTag) []string {
			sqlxTag := io.ParseTag(tag)
			if sqlxTag.Column == "" {
				return []string{name}
			}
			return strings.Split(sqlxTag.Column, "|")
		}),
	)

	// Build selectors on OUTER using "Actual.<candidate>" (codec targets)
	for _, column := range columns {
		var sel *structology.Selector
		candidates := []string{
			column.Name,                  // exact alias
			strings.ToLower(column.Name), // lowercase alias
			toUpperCamel(column.Name),    // Go-style name
		}
		for _, cand := range candidates {
			if cand == "" {
				continue
			}
			path := actualFieldName + "." + cand
			sel = stateType.Lookup(path)
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
	// OUTER wrapper pointer (used by xunsafe/selectors)
	outerPtr := xunsafe.AsPointer(value)

	// 1) Ensure OUTER.Actual is non-nil if Actual is a pointer type
	if c.actualIsPtr {
		curr := c.unwrapperActual.Value(outerPtr) // interface{} of *Elem or nil
		needsAlloc := false
		if curr == nil {
			needsAlloc = true
		} else {
			rv := reflect.ValueOf(curr)
			if rv.Kind() == reflect.Ptr && rv.IsNil() {
				needsAlloc = true
			}
		}
		if needsAlloc {
			if c.actualElemTyp == nil {
				return fmt.Errorf("invalid Actual element type")
			}
			newVal := reflect.New(c.actualElemTyp).Interface() // *Elem
			c.unwrapperActual.SetValue(outerPtr, newVal)
		}
	}

	// 2) SAFE Shadow -> Actual copy via reflect (avoid unsafe header corruption)
	// Build a live reflect.Value view over OUTER
	outerRV := reflect.NewAt(c.actualType, outerPtr).Elem()

	// shadowRV is the embedded Shadow struct value
	shadowRV := outerRV.FieldByName(shadowFieldName)

	// actualRV is the Actual field (struct or pointer-to-struct)
	actualRV := outerRV.FieldByName(actualFieldName)
	var actualElemRV reflect.Value
	if c.actualIsPtr {
		// ensure non-nil (already ensured above)
		if actualRV.IsNil() {
			actualRV.Set(reflect.New(c.actualElemTyp))
		}
		actualElemRV = actualRV.Elem()
	} else {
		actualElemRV = actualRV
	}

	// Copy exported fields by name
	for _, name := range c.shadowFieldNames {
		dst := actualElemRV.FieldByName(name)
		if !dst.IsValid() || !dst.CanSet() {
			continue
		}
		src := shadowRV.FieldByName(name)
		if !src.IsValid() {
			continue
		}
		if src.Type().AssignableTo(dst.Type()) {
			dst.Set(src)
			continue
		}
		if src.Type().ConvertibleTo(dst.Type()) {
			dst.Set(src.Convert(dst.Type()))
			continue
		}
		// Otherwise, skip incompatible types (codec may overwrite later)
	}

	// 3) Apply codecs: read raw DB value from promoted Col{i} on OUTER, decode, and set via selectors
	for i, column := range c.columns {
		if c.fields[i] == nil {
			return fmt.Errorf("codec raw field not found for column %q", column.Name)
		}
		if c.selectors[i] == nil {
			return fmt.Errorf("codec selector not found for column %q (tried Actual.<alias|lower|camel>)", column.Name)
		}
		raw := c.fields[i].Value(outerPtr)
		decoded, err := column.Codec.Transform(ctx, raw, codec.WithOptions(record))
		if err != nil {
			return err
		}
		// Selector root is OUTER (path "Actual.<...>")
		if err = c.selectors[i].SetValue(outerPtr, decoded); err != nil {
			return err
		}
	}
	return nil
}

// toUpperCamel converts snake/space/hyphen/dot separated names to UpperCamel.
// "AD_ORDERS_DATA_INDEX" -> "AdOrdersDataIndex"
func toUpperCamel(s string) string {
	if s == "" {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	capNext := true
	for _, r := range s {
		switch r {
		case '_', '-', ' ', '.':
			capNext = true
			continue
		}
		if capNext {
			b.WriteRune(unicode.ToUpper(r))
			capNext = false
		} else {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}
