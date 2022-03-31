package data

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"unsafe"
)

//Visitor represents visitor function
type Visitor func(value interface{}) error

//Collector collects and build result from data fetched from Database
//If View or any of the View.With MatchStrategy support Parallel fetching, it is important to call MergeData
//when all needed data was fetched
type Collector struct {
	mutex  sync.Mutex
	parent *Collector

	dest          interface{}
	appender      *xunsafe.Appender
	valuePosition map[string]map[interface{}][]int //stores positions in main slice, based on field name, indexed by field value.
	types         map[string]*xunsafe.Type
	relation      *Relation

	values map[string]*[]interface{} //acts like a buffer. Value resolved with Resolve method can't be put to the value position map
	// because value fetched from database was not scanned into yet. Putting value to the map as a key, would create key as a pointer to the zero value.

	slice     *xunsafe.Slice
	view      *View
	relations []*Collector

	wg              *sync.WaitGroup
	supportParallel bool
	wgDelta         int
}

func (r *Collector) Lock() *sync.Mutex {
	if r.parent == nil {
		return &r.mutex
	}
	return &r.parent.mutex
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
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Int32, reflect.Uint16, reflect.Int16, reflect.Uint8, reflect.Int8:
		scanType = reflect.TypeOf(0)
	}
	r.types[column.Name()] = xunsafe.NewType(scanType)
	return func(ptr unsafe.Pointer) interface{} {
		var valuePtr interface{}
		switch kind {
		case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Int32, reflect.Uint16, reflect.Int16, reflect.Uint8, reflect.Int8:
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

//parentValuesPositions returns positions in the parent main slice by given column name
//After first use, it is not possible to index new resolved column indexes by Resolve method
func (r *Collector) parentValuesPositions(columnName string) map[interface{}][]int {
	result, ok := r.parent.valuePosition[columnName]
	if !ok {
		r.indexParentPositions(columnName)
		result = r.parent.valuePosition[columnName]
	}
	return result
}

//NewCollector creates a collector
func NewCollector(slice *xunsafe.Slice, view *View, dest interface{}, supportParallel bool) *Collector {
	ensuredDest := ensureDest(dest, view)
	wg := sync.WaitGroup{}
	wgDelta := 0
	if !supportParallel {
		wgDelta = 1
	}

	wg.Add(wgDelta)

	return &Collector{
		dest:            ensuredDest,
		valuePosition:   make(map[string]map[interface{}][]int),
		appender:        slice.Appender(xunsafe.AsPointer(ensuredDest)),
		slice:           slice,
		view:            view,
		types:           make(map[string]*xunsafe.Type),
		values:          make(map[string]*[]interface{}),
		supportParallel: supportParallel,
		wg:              &wg,
		wgDelta:         wgDelta,
	}
}

func ensureDest(dest interface{}, view *View) interface{} {
	if _, ok := dest.(*interface{}); ok {
		return reflect.MakeSlice(view.Schema.SliceType(), 0, 1).Interface()
	}
	return dest
}

//Visitor creates visitor function
func (r *Collector) Visitor() Visitor {
	relation := r.relation
	visitorRelations := RelationsSlice(r.view.With).PopulateWithVisitor()
	for _, rel := range visitorRelations {
		r.valuePosition[rel.Column] = map[interface{}][]int{}
	}

	visitors := make([]Visitor, 1)
	visitors[0] = r.valueIndexer(visitorRelations)

	if relation != nil && (r.parent == nil || !r.parent.SupportsParallel()) {
		switch relation.Cardinality {
		case "One":
			visitors = append(visitors, r.visitorOne(relation))
		case "Many":
			visitors = append(visitors, r.visitorMany(relation))
		}
	}

	return func(value interface{}) error {
		var err error
		for _, visitor := range visitors {
			if err = visitor(value); err != nil {
				return err
			}
		}
		return nil
	}
}

func (r *Collector) valueIndexer(visitorRelations []*Relation) func(value interface{}) error {
	counter := 0
	return func(value interface{}) error {
		ptr := xunsafe.AsPointer(value)
		for _, rel := range visitorRelations {
			fieldValue := rel.columnField.Value(ptr)
			r.indexValueByRel(fieldValue, rel, counter)
		}
		counter++
		return nil
	}
}

func (r *Collector) indexValueByRel(fieldValue interface{}, rel *Relation, counter int) {
	switch acutal := fieldValue.(type) {
	case []int:
		for _, v := range acutal {
			r.indexValueToPostition(rel, v, counter)
		}
	case []string:
		for _, v := range acutal {
			r.indexValueToPostition(rel, v, counter)
		}
	default:
		r.indexValueToPostition(rel, fieldValue, counter)
	}
}

func (r *Collector) indexValueToPostition(rel *Relation, fieldValue interface{}, counter int) {
	_, ok := r.valuePosition[rel.Column][fieldValue]
	if !ok {
		r.valuePosition[rel.Column][fieldValue] = []int{counter}
	} else {
		r.valuePosition[rel.Column][fieldValue] = append(r.valuePosition[rel.Column][fieldValue], counter)
	}
}

func (r *Collector) visitorOne(relation *Relation) func(value interface{}) error {
	keyField := relation.Of.field
	holderField := relation.holderField
	dest := r.parent.Dest()
	destPtr := xunsafe.AsPointer(dest)
	var key interface{}

	return func(owner interface{}) error {
		key = keyField.Interface(xunsafe.AsPointer(owner))
		valuePosition := r.parentValuesPositions(relation.Column)
		positions, ok := valuePosition[key]
		if !ok {
			return nil
		}

		for _, index := range positions {
			item := r.parent.slice.ValuePointerAt(destPtr, index)
			holderField.SetValue(xunsafe.AsPointer(item), owner)
		}
		return nil
	}
}

func (r *Collector) visitorMany(relation *Relation) func(value interface{}) error {
	keyField := relation.Of.field
	holderField := relation.holderField
	counter := 0
	var xType *xunsafe.Type
	var values *[]interface{}
	var key interface{}
	dest := r.parent.Dest()
	destPtr := xunsafe.AsPointer(dest)

	return func(owner interface{}) error {
		if keyField == nil && xType == nil {
			xType = r.types[relation.Of.Column]
			values = r.values[relation.Of.Column]
		}

		if keyField != nil {
			key = keyField.Interface(xunsafe.AsPointer(owner))
		} else {
			key = xType.Deref((*values)[counter])
			counter++
		}

		valuePosition := r.parentValuesPositions(relation.Column)
		positions, ok := valuePosition[key]
		if !ok {
			return nil
		}

		for _, index := range positions {
			parentItem := r.parent.slice.ValuePointerAt(destPtr, index)
			r.Lock().Lock()
			sliceAddPtr := holderField.Pointer(xunsafe.AsPointer(parentItem))
			slice := relation.Of.Schema.Slice()
			appender := slice.Appender(sliceAddPtr)
			appender.Append(owner)
			r.Lock().Unlock()
			r.view.Logger.ObjectReconciling(dest, owner, parentItem, index)
		}

		return nil
	}
}

//NewItem creates and return item provider
//Each produced item is automatically appended to the dest
func (r *Collector) NewItem() func() interface{} {
	return func() interface{} {
		return r.appender.Add()
	}
}

func (r *Collector) indexParentPositions(name string) {
	if r.parent == nil {
		return
	}

	values := r.parent.values[name]
	if values == nil {
		return
	}

	xType := r.parent.types[name]
	r.parent.valuePosition[name] = map[interface{}][]int{}
	for position, v := range *values {
		val := xType.Deref(v)
		_, ok := r.parent.valuePosition[name][val]
		if !ok {
			r.parent.valuePosition[name][val] = make([]int, 0)
		}

		r.parent.valuePosition[name][val] = append(r.parent.valuePosition[name][val], position)
	}
}

//Relations creates and register new Collector for each Relation present in the Selector.Columns if View allows use Selector.Columns
func (r *Collector) Relations(selector *Selector) []*Collector {
	result := make([]*Collector, len(r.view.With))

	counter := 0
	for i := range r.view.With {
		if r.view.CanUseSelectorColumns() && selector != nil && !selector.Has(r.view.With[i].Holder) {
			continue
		}

		dest := reflect.MakeSlice(r.view.With[counter].Of.View.Schema.SliceType(), 0, 1).Interface()
		slice := r.view.With[counter].Of.View.Schema.Slice()
		wg := sync.WaitGroup{}

		delta := 0
		if !r.SupportsParallel() {
			delta = 1
		}
		wg.Add(delta)

		result[counter] = &Collector{
			parent:          r,
			dest:            dest,
			appender:        slice.Appender(xunsafe.AsPointer(dest)),
			valuePosition:   make(map[string]map[interface{}][]int),
			types:           make(map[string]*xunsafe.Type),
			values:          make(map[string]*[]interface{}),
			slice:           slice,
			view:            &r.view.With[counter].Of.View,
			relation:        r.view.With[counter],
			supportParallel: r.view.With[counter].Of.MatchStrategy.SupportsParallel(),
			wg:              &wg,
			wgDelta:         delta,
		}
		counter++
	}

	r.relations = result[:counter]
	return result[:counter]
}

//View returns View assigned to the Collector
func (r *Collector) View() *View {
	return r.view
}

//Dest returns collector slice
func (r *Collector) Dest() interface{} {
	return r.dest
}

//SupportsParallel if Collector supports parallelism, it means that his Relations can fetch data in the same time
//Later on it will be merged with the parent Collector
func (r *Collector) SupportsParallel() bool {
	return r.supportParallel
}

//MergeData merges data with Collectors produced via Relations
//It is sufficient to call it on the most Parent Collector to produce result
func (r *Collector) MergeData() {
	for i := range r.relations {
		r.relations[i].MergeData()
	}

	if r.parent == nil || !r.parent.SupportsParallel() {
		return
	}

	r.mergeToParent()
}

func (r *Collector) mergeToParent() {
	valuePositions := r.parentValuesPositions(r.relation.Column)
	destPtr := xunsafe.AsPointer(r.dest)
	field := r.relation.Of.field
	holderField := r.relation.holderField
	parentSlice := r.parent.slice
	parentDestPtr := xunsafe.AsPointer(r.parent.dest)

	for i := 0; i < r.slice.Len(destPtr); i++ {
		value := r.slice.ValuePointerAt(destPtr, i)
		key := field.Value(xunsafe.AsPointer(value))
		positions, ok := valuePositions[key]
		if !ok {
			continue
		}

		for _, position := range positions {
			parentValue := parentSlice.ValuePointerAt(parentDestPtr, position)
			if r.relation.Cardinality == One {
				at := r.slice.ValuePointerAt(destPtr, i)
				holderField.SetValue(xunsafe.AsPointer(parentValue), at)
			} else if r.relation.Cardinality == Many {
				r.Lock().Lock()
				appender := r.slice.Appender(holderField.ValuePointer(xunsafe.AsPointer(parentValue)))
				appender.Append(value)
				r.Lock().Unlock()
				r.view.Logger.ObjectReconciling(r.dest, value, parentValue, position)
			}
		}
	}
}

//ParentPlaceholders if Collector doesn't support parallel fetching and has a Parent, it will return a parent field values and column name
//that the relation was created from, otherwise empty slice and empty string
//i.e. if Parent Collector collects Employee{AccountId: int}, Column.Name is account_id and Collector collects Account
//it will extract and return all the AccountId that were accumulated and account_id
func (r *Collector) ParentPlaceholders() ([]interface{}, string) {
	if r.parent == nil || r.parent.SupportsParallel() {
		return []interface{}{}, ""
	}

	column := r.relation.Of.Column

	if r.relation.columnField != nil {
		destPtr := xunsafe.AsPointer(r.parent.dest)
		sliceLen := r.parent.slice.Len(destPtr)
		result := make([]interface{}, 0)
		for i := 0; i < sliceLen; i++ {
			parent := r.parent.slice.ValuePointerAt(destPtr, i)
			fieldValue := r.relation.columnField.Value(xunsafe.AsPointer(parent))
			switch actual := fieldValue.(type) {
			case []int:
				for j := range actual {
					result = append(result, actual[j])
				}
			case []string:
				for j := range actual {
					result = append(result, actual[j])
				}
			default:
				result = append(result, fieldValue)
			}
		}

		return result, column
	} else {
		positions := r.parentValuesPositions(r.relation.Column)
		result := make([]interface{}, len(positions))
		counter := 0

		for key := range positions {
			result[counter] = key
			counter++
		}
		return result, column

	}
}

func (r *Collector) WaitIfNeeded() {
	if r.parent != nil {
		r.parent.wg.Wait()
	}
}

func (r *Collector) Fetched() {
	if r.wgDelta > 0 {
		r.wg.Done()
		r.wgDelta--
	}
}
