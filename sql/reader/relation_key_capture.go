package reader

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/viant/datly/sql/reader/collector"
	sqlxio "github.com/viant/sqlx/io"
	sqlxread "github.com/viant/sqlx/io/read"
)

type relationKeyCapture struct {
	collector *collector.Collector
	columns   map[string]bool
	pending   []relationKeySnapshot
}

type relationKeySnapshot struct {
	column      string
	destination any
	slot        *any
}

func (c *relationKeyCapture) mapper(columns []sqlxio.Column, rowType reflect.Type, resolver sqlxio.Resolve, options ...sqlxread.Option) (sqlxread.RowMapper, error) {
	unmapped := map[string]bool{}
	resolve := sqlxio.Resolve(func(column sqlxio.Column) func(unsafe.Pointer) any {
		unmapped[column.Name()] = true
		return resolver(column)
	})
	var mapper sqlxread.RowMapper
	var err error
	for rowType.Kind() == reflect.Ptr {
		rowType = rowType.Elem()
	}
	if rowType.Kind() == reflect.Struct {
		mapper, err = sqlxread.NewSQLStructMapper(columns, rowType, resolve, options...)
	} else {
		mapper, err = sqlxread.GenericRowMapper(columns)
	}
	if err != nil {
		return nil, err
	}
	var indexes []int
	for i, column := range columns {
		if c.columns[column.Name()] && !unmapped[column.Name()] {
			indexes = append(indexes, i)
		}
	}
	if len(indexes) == 0 {
		c.pending = nil
		return mapper, nil
	}
	return func(row any) ([]any, error) {
		values, err := mapper(row)
		if err != nil {
			return nil, err
		}
		c.pending = c.pending[:0]
		for _, index := range indexes {
			name := columns[index].Name()
			c.pending = append(c.pending, relationKeySnapshot{column: name, destination: values[index], slot: c.collector.ReserveSQLKey(name)})
		}
		return values, nil
	}, nil
}

func (c *relationKeyCapture) snapshot() error {
	for _, pending := range c.pending {
		value, err := scannedRelationKey(pending.destination)
		if err != nil {
			return fmt.Errorf("capture SQL relation key %s: %w", pending.column, err)
		}
		*pending.slot = value
	}
	return nil
}

// Preserve the scanned value, not a pointer into the row that OnFetch may
// mutate. SQLX still owns conversion, encoded fields, and cache decoding.
func scannedRelationKey(destination any) (any, error) {
	value := reflect.ValueOf(destination)
	for value.IsValid() {
		if (value.Kind() == reflect.Ptr || value.Kind() == reflect.Interface) && value.IsNil() {
			return nil, nil
		}
		if valuer, ok := value.Interface().(driver.Valuer); ok {
			actual, err := valuer.Value()
			if err != nil {
				return nil, err
			}
			return sqlxio.NormalizeKey(actual), nil
		}
		if value.Kind() != reflect.Ptr && value.Kind() != reflect.Interface {
			break
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return nil, nil
	}
	key := sqlxio.NormalizeKey(value.Interface())
	if key != nil && !reflect.TypeOf(key).Comparable() {
		return nil, fmt.Errorf("unsupported scanned key type %T", key)
	}
	return key, nil
}
