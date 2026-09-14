package dml

import (
	"reflect"
	"strings"
)

// This file owns the reflection predicate that decides whether an insert row
// carries an unset generated-ID field. canBatchInsert (dml.go) uses it to skip
// multi-row batching when a row would need its auto-generated ID backfilled,
// which batched inserts cannot return per-row.

func insertNeedsGeneratedIDBackfill(data any) bool {
	value := reflect.ValueOf(data)
	for value.IsValid() && value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return false
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return false
	}
	valueType := value.Type()
	for i := 0; i < valueType.NumField(); i++ {
		field := valueType.Field(i)
		if !field.IsExported() || !isGeneratedIDField(field) {
			continue
		}
		return isZeroInsertIDValue(value.Field(i))
	}
	return false
}

func isGeneratedIDField(field reflect.StructField) bool {
	if kind, ok := integerFieldKind(field.Type); ok {
		_ = kind
		sqlxTag := field.Tag.Get("sqlx")
		if strings.Contains(sqlxTag, "primaryKey") {
			return true
		}
		return field.Name == "ID" || field.Name == "Id"
	}
	return false
}

func integerFieldKind(fieldType reflect.Type) (reflect.Kind, bool) {
	switch fieldType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fieldType.Kind(), true
	case reflect.Ptr:
		elemKind := fieldType.Elem().Kind()
		switch elemKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return elemKind, true
		}
	}
	return reflect.Invalid, false
}

func isZeroInsertIDValue(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}
	if value.Kind() == reflect.Ptr {
		return value.IsNil()
	}
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int() == 0
	}
	return false
}
