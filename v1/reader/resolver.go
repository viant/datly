package reader

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"reflect"
	"unsafe"
)

type ColumnResolver struct {
	values map[interface{}]int
}

func (r *ColumnResolver) Value(value interface{}) (int, bool) {
	if val, ok := r.values[value]; ok {
		return val, true
	}
	return 0, false
}

//Resolver represents unmatched column resolver
type Resolver struct {
	allowedColumns   map[string]bool
	columnsResolvers map[string]*ColumnResolver
	data             map[string][]interface{}
	err              error
}

//Resolve resolved unmapped column
func (r *Resolver) Resolve(column io.Column) func(ptr unsafe.Pointer) interface{} {
	return func(ptr unsafe.Pointer) interface{} {
		value := reflect.New(column.ScanType())
		result := value.Interface()
		if !r.columnAllowed(column) {
			r.err = fmt.Errorf("can't resolve column %v", column.Name())
		} else {
			defer r.addPtr(column.Name(), value.Interface())
		}
		return result
	}
}

func (r *Resolver) addPtr(columnName string, result interface{}) {
	if _, ok := r.data[columnName]; ok {
		r.data[columnName] = append(r.data[columnName], result)
		return
	}

	r.data[columnName] = []interface{}{result}
}

func (r *Resolver) columnAllowed(column io.Column) bool {
	if r.allowedColumns == nil {
		return true
	}

	if _, ok := r.allowedColumns[column.Name()]; ok {
		return true
	}
	return false
}

func (r *Resolver) ColumnResolver(columnName string) *ColumnResolver {
	r.ensureColumnResolver(columnName)
	return r.columnsResolvers[columnName]
}

func (r *Resolver) ensureColumnResolver(name string) {
	if _, ok := r.columnsResolvers[name]; ok {
		return
	}

	dataMapified := make(map[interface{}]int)
	var dereference func(interface{}) interface{}
	for key := range r.data[name] {
		objValue := r.data[name][key]
		if dereference == nil {
			dereference = dereferencer(objValue)
		}
		dataMapified[dereference(objValue)] = key
	}

	r.columnsResolvers[name] = &ColumnResolver{
		values: dataMapified,
	}
}

//NewResolver creates a resolver
func NewResolver(columns []string) *Resolver {
	var allowedColumns map[string]bool
	if len(columns) != 0 {
		allowedColumns = make(map[string]bool)
		for i := range columns {
			allowedColumns[columns[i]] = true
		}
	}

	return &Resolver{
		allowedColumns:   allowedColumns,
		columnsResolvers: map[string]*ColumnResolver{},
		data:             map[string][]interface{}{},
	}
}
