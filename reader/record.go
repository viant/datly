package reader

import (
	"github.com/viant/datly/generic"
)

//Record represents a record
type Record struct {
	proto         *generic.Proto
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
func (r *Record) Object() (*generic.Object, error) {
	var objectValues = make([]interface{}, len(r.columns))
	copy(objectValues, r.values)
	return r.proto.Object(objectValues)
}

//NewRecord creates a record
func NewRecord(proto *generic.Proto, columns []string) *Record {
	result := &Record{
		proto:         proto,
		columns:       columns,
		values:        make([]interface{}, len(columns)),
		valuePointers: make([]interface{}, len(columns)),
	}
	for i := range columns {
		proto.FieldWithValue(columns[i], nil)
	}
	return result
}
