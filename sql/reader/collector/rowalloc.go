package collector

import (
	"reflect"
	"unsafe"

	"github.com/viant/xunsafe"
)

// Column is the minimal interface the Collector needs from a sql.ColumnType
// wrapper. It mirrors sqlx/io.Column but avoids a hard import of that package
// in callers that don't have sqlx on the class path.
type Column interface {
	Name() string
	ScanType() reflect.Type
}

// Resolve returns a per-row allocator for an unmapped column, buffering the
// scanned values so that the position-index step can use them later.
func (r *Collector) Resolve(column Column) func(ptr unsafe.Pointer) interface{} {
	buffer, ok := r.values[column.Name()]
	if !ok {
		localSlice := make([]interface{}, 0)
		buffer = &localSlice
		r.values[column.Name()] = buffer
	}

	scanType := column.ScanType()
	kind := scanType.Kind()
	switch kind {
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64,
		reflect.Uint32, reflect.Int32, reflect.Uint16, reflect.Int16,
		reflect.Uint8, reflect.Int8:
		scanType = reflect.TypeOf(0)
	}
	r.types[column.Name()] = xunsafe.NewType(scanType)

	return func(ptr unsafe.Pointer) interface{} {
		var valuePtr interface{}
		switch kind {
		case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64,
			reflect.Uint32, reflect.Int32, reflect.Uint16, reflect.Int16,
			reflect.Uint8, reflect.Int8:
			value := 0
			valuePtr = &value
		case reflect.Float64, reflect.Float32:
			value := 0.0
			valuePtr = &value
		case reflect.Bool:
			value := false
			valuePtr = &value
		case reflect.String:
			value := ""
			valuePtr = &value
		default:
			valuePtr = reflect.New(scanType).Interface()
		}
		*buffer = append(*buffer, valuePtr)
		return valuePtr
	}
}

// NewItem returns a factory that allocates and appends a typed model row.
func (r *Collector) NewItem() func() interface{} {
	return func() interface{} {
		return r.appender.Add()
	}
}

// Append adds a decoded typed row to the collector destination.
func (r *Collector) Append(value interface{}) {
	r.appender.Append(value)
}
