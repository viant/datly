package json

import "reflect"

type groupedFields struct {
	inlinable      []reflect.StructField
	presenceFields []reflect.StructField
	regularFields  []reflect.StructField
}
