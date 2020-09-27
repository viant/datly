package reader

import (
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/dsc"
	"github.com/viant/gtly"
	"strings"
	"time"
)

//Record represents a record
type Record struct {
	proto         *gtly.Proto
	columns       []string
	values        []interface{}
	valuePointers []interface{}
}

//Reset reset record
func (r *Record) Reset() {
	for i := range r.columns {
		r.values[i] = nil
		r.valuePointers[i] = &r.values[i]
	}
}

//Object returns record object
func (r *Record) Object() (*gtly.Object, error) {
	var objectValues = make([]interface{}, len(r.columns))
	copy(objectValues, r.values)
	return r.proto.Object(objectValues)
}

//NewRecord creates a record
func NewRecord(proto *gtly.Proto, columns []string, columnTypes []dsc.ColumnType) *Record {
	result := &Record{
		proto:         proto,
		columns:       columns,
		values:        make([]interface{}, len(columns)),
		valuePointers: make([]interface{}, len(columns)),
	}

	for i := range columns {
		value := getColumnTypeValue(i, columnTypes)
		proto.FieldWithValue(columns[i], value)
	}
	return result
}

func getColumnTypeValue(i int, types []dsc.ColumnType) interface{} {
	if i >= len(types) {
		return 0
	}
	dbType := types[i].DatabaseTypeName()
	isArray := strings.HasPrefix(dbType, "[]")
	if isArray {
		dbType = string(dbType[2:])
	}
	if index := strings.Index(dbType, "("); index != -1 {
		dbType = string(dbType[:index])
	}
	switch strings.ToUpper(dbType) {
	case shared.ColumnTypeBit, shared.ColumnTypeBoolean, shared.ColumnTypeTinyInt:
		return false
	case shared.ColumnTypeInt, shared.ColumnTypeInteger, shared.ColumnTypeInt64, shared.ColumnTypeSmallInt, shared.ColumnTypeBigInt:
		return int(0)
	case shared.ColumnTypeDecimal, shared.ColumnTypeFloat, shared.ColumnTypeFloat64, shared.ColumnTypeNumeric, shared.ColumnTypeNumber:
		return float64(0.0)
	case shared.ColumnTypeDate, shared.ColumnTypeDateTime, shared.ColumnTypeTimestamp, shared.ColumnTypeTimestampTz:
		return time.Now()
	case shared.ColumnTypeChar, shared.ColumnTypeVarchar, shared.ColumnTypeVarchar2, shared.ColumnTypeString, shared.ColumnTypeCBlob, shared.ColumnTypeText:
		return ""
	default:
		fmt.Printf("unsupported mapping type: %v\n", dbType)
	}

	return nil
}
