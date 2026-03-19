package view

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"time"

	codec2 "github.com/viant/datly/view/extension/codec"
	"github.com/viant/sqlx/io"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
)

const (
	rawFieldName    = "Raw"
	actualFieldName = "Actual"
)

var (
	sqlNullBoolType   = reflect.TypeOf(sql.NullBool{})
	sqlNullFloatType  = reflect.TypeOf(sql.NullFloat64{})
	sqlNullIntType    = reflect.TypeOf(sql.NullInt64{})
	sqlNullStringType = reflect.TypeOf(sql.NullString{})
	timePtrType       = reflect.TypeOf((*time.Time)(nil))
	sqlNullTimeType   = reflect.TypeOf(sql.NullTime{})
)

type (
	columnsCodec struct {
		sourceFields []*xunsafe.Field
		targetFields []*xunsafe.Field
		unwrapper    *xunsafe.Field
		actualType   reflect.Type
		columns      []*Column
	}
)

func newColumnsCodec(viewType reflect.Type, columns []*Column) (*columnsCodec, error) {
	hasCodec := false
	for _, column := range columns {
		if column.Codec != nil || requiresSyntheticScan(column) {
			hasCodec = true
			break
		}
	}

	if !hasCodec {
		return nil, nil
	}

	codec := &columnsCodec{}

	if err := codec.init(viewType, columns); err != nil {
		return nil, err
	}

	return codec, nil
}

func (c *columnsCodec) init(viewType reflect.Type, columns []*Column) error {
	c.columns = columns
	targetType := viewType
	if targetType.Kind() == reflect.Ptr {
		targetType = targetType.Elem()
	}
	targetFields := make([]*xunsafe.Field, len(columns))
	for i, column := range columns {
		targetFields[i] = lookupColumnField(targetType, column)
	}
	codecStructFields := make([]reflect.StructField, len(columns))
	for i, column := range columns {
		scanType := columnDatabaseScanType(column, targetFields[i])
		codecStructFields[i] = reflect.StructField{
			Name: "Col" + strconv.Itoa(i),
			Type: scanType,
			Tag:  reflect.StructTag(fmt.Sprintf(`sqlx:"%v"`, column.Name)),
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
			Name: actualFieldName,
			Type: viewType,
		},
	})

	c.sourceFields = make([]*xunsafe.Field, len(columns))
	for i := 0; i < len(columns); i++ {
		c.sourceFields[i] = xunsafe.FieldByIndex(rawType, i)
	}

	c.unwrapper = xunsafe.FieldByIndex(c.actualType, 1)
	c.targetFields = targetFields
	return nil
}

func lookupColumnField(viewType reflect.Type, column *Column) *xunsafe.Field {
	if viewType.Kind() == reflect.Ptr {
		viewType = viewType.Elem()
	}
	if column == nil {
		return nil
	}
	if field := xunsafe.FieldByName(viewType, column.Name); field != nil {
		return field
	}
	if sqlxTag := io.ParseTag(reflect.StructTag(column.Tag)); sqlxTag.Column != "" {
		if field := xunsafe.FieldByName(viewType, sqlxTag.Column); field != nil {
			return field
		}
	}
	for i := 0; i < viewType.NumField(); i++ {
		structField := viewType.Field(i)
		sqlxTag := io.ParseTag(structField.Tag)
		if sqlxTag.Column == column.Name {
			return xunsafe.FieldByIndex(viewType, i)
		}
	}
	return nil
}

func requiresSyntheticScan(column *Column) bool {
	if column == nil {
		return false
	}
	if column.Codec != nil {
		return true
	}
	sqlxTag := io.ParseTag(reflect.StructTag(column.Tag))
	if sqlxTag != nil && sqlxTag.Transient {
		return true
	}
	return false
}

func columnDatabaseScanType(column *Column, targetField *xunsafe.Field) reflect.Type {
	if column == nil {
		return reflect.TypeOf([]byte{})
	}
	if targetField == nil {
		return reflect.TypeOf([]byte{})
	}
	if scanType, ok := nullableScanType(targetField.Type); ok {
		return scanType
	}
	if column.Codec != nil && column.Codec.Name == codec2.JSON {
		return reflect.TypeOf([]byte{})
	}
	return column.ColumnType()
}

func nullableScanType(targetType reflect.Type) (reflect.Type, bool) {
	if targetType == nil || targetType.Kind() != reflect.Ptr {
		return nil, false
	}
	if targetType == timePtrType {
		return sqlNullTimeType, true
	}
	switch targetType.Elem().Kind() {
	case reflect.String:
		return sqlNullStringType, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return sqlNullIntType, true
	case reflect.Float32, reflect.Float64:
		return sqlNullFloatType, true
	case reflect.Bool:
		return sqlNullBoolType, true
	}
	return nil, false
}

func (c *columnsCodec) updateValue(ctx context.Context, value interface{}, record *codec2.ParentValue) error {
	asPtr := xunsafe.AsPointer(value)
	actualPtr := c.unwrapper.ValuePointer(asPtr)
	actualType := c.unwrapper.Type
	if actualType.Kind() == reflect.Ptr {
		if c.unwrapper.IsNil(asPtr) {
			c.unwrapper.SetValue(asPtr, reflect.New(actualType.Elem()).Interface())
			actualPtr = c.unwrapper.ValuePointer(asPtr)
		}
		actualType = actualType.Elem()
	}
	for i, column := range c.columns {
		fieldValue := c.sourceFields[i].Value(asPtr)
		decoded := fieldValue
		if column.Codec != nil {
			var err error
			decoded, err = column.Codec.Transform(ctx, fieldValue, codec.WithOptions(record))
			if err != nil {
				return err
			}
		}
		targetField := c.targetFields[i]
		if targetField == nil {
			continue
		}
		if targetField.Type.Kind() == reflect.Ptr {
			pointerValue, handled, err := nullablePointerValue(decoded, targetField.Type)
			if err != nil {
				return err
			}
			if handled {
				targetField.SetValue(actualPtr, pointerValue)
				continue
			}
		}
		targetField.SetValue(actualPtr, decoded)
	}

	return nil
}

func nullablePointerValue(decoded interface{}, targetType reflect.Type) (interface{}, bool, error) {
	if decoded == nil {
		return reflect.Zero(targetType).Interface(), true, nil
	}
	switch actual := decoded.(type) {
	case sql.NullString:
		return nullablePointerFromString(actual, targetType)
	case sql.NullInt64:
		return nullablePointerFromInt(actual, targetType)
	case sql.NullFloat64:
		return nullablePointerFromFloat(actual, targetType)
	case sql.NullBool:
		return nullablePointerFromBool(actual, targetType)
	case sql.NullTime:
		return nullablePointerFromTime(actual, targetType)
	}
	return nil, false, nil
}

func nullablePointerFromString(decoded sql.NullString, targetType reflect.Type) (interface{}, bool, error) {
	if targetType.Kind() != reflect.Ptr || targetType.Elem().Kind() != reflect.String {
		return nil, false, nil
	}
	if !decoded.Valid {
		return reflect.Zero(targetType).Interface(), true, nil
	}
	value := reflect.New(targetType.Elem())
	value.Elem().SetString(decoded.String)
	return value.Interface(), true, nil
}

func nullablePointerFromInt(decoded sql.NullInt64, targetType reflect.Type) (interface{}, bool, error) {
	if targetType.Kind() != reflect.Ptr {
		return nil, false, nil
	}
	elem := targetType.Elem()
	switch elem.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !decoded.Valid {
			return reflect.Zero(targetType).Interface(), true, nil
		}
		value := reflect.New(elem)
		if value.Elem().OverflowInt(decoded.Int64) {
			return nil, true, fmt.Errorf("failed to assign %d to %v", decoded.Int64, targetType)
		}
		value.Elem().SetInt(decoded.Int64)
		return value.Interface(), true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if !decoded.Valid {
			return reflect.Zero(targetType).Interface(), true, nil
		}
		if decoded.Int64 < 0 {
			return nil, true, fmt.Errorf("failed to assign negative value %d to %v", decoded.Int64, targetType)
		}
		value := reflect.New(elem)
		uintValue := uint64(decoded.Int64)
		if value.Elem().OverflowUint(uintValue) {
			return nil, true, fmt.Errorf("failed to assign %d to %v", decoded.Int64, targetType)
		}
		value.Elem().SetUint(uintValue)
		return value.Interface(), true, nil
	}
	return nil, false, nil
}

func nullablePointerFromFloat(decoded sql.NullFloat64, targetType reflect.Type) (interface{}, bool, error) {
	if targetType.Kind() != reflect.Ptr {
		return nil, false, nil
	}
	elem := targetType.Elem()
	switch elem.Kind() {
	case reflect.Float32, reflect.Float64:
		if !decoded.Valid {
			return reflect.Zero(targetType).Interface(), true, nil
		}
		value := reflect.New(elem)
		if value.Elem().OverflowFloat(decoded.Float64) {
			return nil, true, fmt.Errorf("failed to assign %f to %v", decoded.Float64, targetType)
		}
		value.Elem().SetFloat(decoded.Float64)
		return value.Interface(), true, nil
	}
	return nil, false, nil
}

func nullablePointerFromBool(decoded sql.NullBool, targetType reflect.Type) (interface{}, bool, error) {
	if targetType.Kind() != reflect.Ptr || targetType.Elem().Kind() != reflect.Bool {
		return nil, false, nil
	}
	if !decoded.Valid {
		return reflect.Zero(targetType).Interface(), true, nil
	}
	value := reflect.New(targetType.Elem())
	value.Elem().SetBool(decoded.Bool)
	return value.Interface(), true, nil
}

func nullablePointerFromTime(decoded sql.NullTime, targetType reflect.Type) (interface{}, bool, error) {
	if targetType != timePtrType {
		return nil, false, nil
	}
	if !decoded.Valid {
		return (*time.Time)(nil), true, nil
	}
	value := decoded.Time
	return &value, true, nil
}
