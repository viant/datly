package data

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

var collectorCounter = 0

//Collector represents unmatched column resolver
type Collector struct {
	parent  *Collector
	session *Session

	appender       *xunsafe.Appender
	allowedColumns map[string]bool
	valuePosition  map[string]map[interface{}][]int
	types          map[string]*xunsafe.Type

	values           map[string]*[]interface{}
	err              error
	slice            *xunsafe.Slice
	view             *View
	collectorCounter int
}

//Resolve resolved unmapped column
func (r *Collector) Resolve(column io.Column) func(ptr unsafe.Pointer) interface{} {
	buffer, ok := r.values[column.Name()]
	if !ok {
		localSlice := make([]interface{}, 0)
		buffer = &localSlice
		r.values[column.Name()] = buffer
	}

	scanType := column.ScanType()
	kind := column.ScanType().Kind()
	switch kind {
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
		scanType = reflect.TypeOf(0)
	}
	r.types[column.Name()] = xunsafe.NewType(scanType)
	return func(ptr unsafe.Pointer) interface{} {
		if !r.columnAllowed(column) {
			r.err = fmt.Errorf("can't resolve column %v", column.Name())
		}

		var valuePtr interface{}
		switch kind {
		case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
			value := 0
			valuePtr = &value
		case reflect.Float64:
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

func (r *Collector) columnAllowed(column io.Column) bool {
	if len(r.allowedColumns) == 0 {
		return true
	}
	if _, ok := r.allowedColumns[column.Name()]; ok {
		return true
	}
	return false
}

func (r *Collector) ValuePosition(columnName string) map[interface{}][]int {
	result, ok := r.valuePosition[columnName]
	if !ok {
		r.indexPositions(columnName)
		result = r.valuePosition[columnName]
	}
	return result
}

//NewCollector creates a collector
func NewCollector(columns []string, slice *xunsafe.Slice, view *View, session *Session) *Collector {
	var allowedColumns map[string]bool
	if len(columns) != 0 {
		allowedColumns = make(map[string]bool)
		for i := range columns {
			allowedColumns[columns[i]] = true
		}
	}

	collectorCounter++

	return &Collector{
		allowedColumns:   allowedColumns,
		valuePosition:    make(map[string]map[interface{}][]int),
		slice:            slice,
		view:             view,
		types:            make(map[string]*xunsafe.Type),
		values:           make(map[string]*[]interface{}),
		collectorCounter: collectorCounter,
		session:          session,
	}
}

// Employee#Id
// Acoount#EmployeeId
func (r *Collector) Visitor(session *Session, view *View) func(value interface{}) error {
	relation := session.RelationOwner(view)
	visitorRelations := Relations(view.With).PopulateWithVisitor()
	if len(visitorRelations) == 0 && relation == nil {
		return func(value interface{}) error {
			return nil
		}
	}

	for _, rel := range visitorRelations {
		r.valuePosition[rel.Column] = map[interface{}][]int{}
	}

	counter := 0
	fn := func(value interface{}) error {
		ptr := xunsafe.AsPointer(value)
		for _, rel := range visitorRelations {
			fieldValue := rel.columnField.Value(ptr)
			_, ok := r.valuePosition[rel.Column][fieldValue]
			if !ok {
				r.valuePosition[rel.Column][fieldValue] = []int{counter}
			} else {
				r.valuePosition[rel.Column][fieldValue] = append(r.valuePosition[rel.Column][fieldValue], counter)
			}
			counter++
		}
		return nil
	}

	if relation == nil {
		return fn
	}

	switch relation.Cardinality {
	case "One":
		return r.visitorOne(session, relation)
	case "Many":
		return r.visitorToMany(session, relation)

		//dest := r.ParentDest()
		//destPtr := xunsafe.AsPointer(dest)
		//valuesIndexed := Index(relation, destPtr, view.Component.Slice(), view.Component.Type())
		//
		//appenders := make(map[interface{}]*xunsafe.Appender)
		//return func(refValue interface{}) error {
		//	id := keyField.Value(xunsafe.AsPointer(refValue))
		//	if appender, ok := appenders[id]; ok {
		//		appender.Append(refValue)
		//		return nil
		//	}
		//	parentRef := valuesIndexed[id]

		//	appender := r.slice.Appender(parentRef)
		//	appenders[id] = appender
		//	appender.Append(refValue)
		return nil
		//}
	}

	return func(owner interface{}) error {
		return nil
	}
}

type Visitor func(value interface{}) error

func (r *Collector) visitorOne(session *Session, relation *Relation, visitors ...Visitor) func(value interface{}) error {
	keyField := relation.Of.field
	holderField := relation.holderField
	return func(owner interface{}) error {
		for i := range visitors {
			if err := visitors[i](owner); err != nil {
				return err
			}
		}

		dest := session.ViewsDest()[r.view.destIndex]
		destPtr := xunsafe.AsPointer(dest)
		if dest == nil {
			return fmt.Errorf("dest was nil")
		}

		key := keyField.Interface(xunsafe.AsPointer(owner))
		valuePosition := r.ValuePosition(relation.Column)
		positions, ok := valuePosition[key]
		if !ok {
			return nil
		}

		for _, index := range positions {
			itemItem := r.slice.ValuePointerAt(destPtr, index)
			holderField.SetValue(xunsafe.AsPointer(itemItem), owner)
		}
		return nil
	}
}

func (r *Collector) visitorToMany(session *Session, relation *Relation, visitors ...Visitor) func(value interface{}) error {
	keyField := relation.Of.field
	holderField := relation.holderField
	return func(owner interface{}) error {
		for i := range visitors {
			if err := visitors[i](owner); err != nil {
				return err
			}
		}

		dest := session.ViewsDest()[r.view.destIndex]
		destPtr := xunsafe.AsPointer(dest)
		if dest == nil {
			return fmt.Errorf("dest was nil")
		}

		key := keyField.Interface(xunsafe.AsPointer(owner))
		valuePosition := r.ValuePosition(relation.Column)
		positions, ok := valuePosition[key]
		if !ok {
			return nil
		}

		for _, index := range positions {
			parentItem := r.slice.ValuePointerAt(destPtr, index)
			sliceAddPtr := holderField.Pointer(xunsafe.AsPointer(parentItem))
			slice := relation.Of.Component.Slice()
			appender := slice.Appender(sliceAddPtr)
			appender.Append(owner)
		}

		return nil
	}
}

func (r *Collector) ensureDest(session *Session, view *View) interface{} {
	dest := session.ViewsDest()[view.destIndex]

	if dest == nil {
		slice := reflect.MakeSlice(r.slice.Type, 0, 0)
		slicePtr := reflect.New(r.slice.Type)
		slicePtr.Elem().Set(slice)
		session.ViewsDest()[view.destIndex] = slicePtr.Interface()
		r.appender = r.slice.Appender(xunsafe.ValuePointer(&slicePtr))
		dest = session.ViewsDest()[view.destIndex]
	} else {
		r.appender = r.slice.Appender(xunsafe.AsPointer(dest))
	}

	return dest
}

func (r *Collector) NewItem(session *Session, view *View) func() interface{} {
	if view.UseTransientSlice() {
		r.ensureDest(session, view)
	}

	return func() interface{} {
		if view.UseTransientSlice() {
			add := r.appender.Add()
			return add
		}
		v := reflect.New(view.DataType().Elem()).Interface()
		return v
	}
}

func (r *Collector) indexPositions(name string) {
	values := r.values[name]
	xType := r.types[name]
	r.valuePosition[name] = map[interface{}][]int{}
	for position, v := range *values {
		val := xType.Deref(v)
		_, ok := r.valuePosition[name][val]
		if !ok {
			r.valuePosition[name][val] = make([]int, 0)
		}

		r.valuePosition[name][val] = append(r.valuePosition[name][val], position)
	}

}
