package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx/io"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"unsafe"
)

// VisitorFn represents visitor function
type VisitorFn func(value interface{}) error

// Collector collects and build result from View fetched from Database
// If View or any of the View.With MatchStrategy support Parallel fetching, it is important to call MergeData
// when all needed View was fetched
type Collector struct {
	mutex         sync.Mutex
	parent        *Collector
	destValue     reflect.Value
	appender      *xunsafe.Appender
	valuePosition map[string]map[string]map[interface{}][]int //stores positions in main slice, based on _field name, indexed by _field value.
	types         map[string]*xunsafe.Type
	relation      *Relation
	dataSync      handler.DataSync
	values        map[string]*[]interface{} //acts like a buffer. Output resolved with Resolve method can't be put to the value position map
	// because value fetched from database was not scanned into yet. Putting value to the map as a key, would create key as a pointer to the zero value.

	slice     *xunsafe.Slice
	view      *View
	relations []*Collector

	wg      *sync.WaitGroup
	readAll bool
	wgDelta int

	indexCounter    int
	manyCounter     int
	codecSlice      *xunsafe.Slice
	codecSliceDest  interface{}
	codecAppender   *xunsafe.Appender
	viewMetaHandler viewSummaryHandlerFn
}

func (r *Collector) SetDest(dest interface{}) {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() == reflect.Ptr {
		r.destValue.Elem().Set(destValue.Elem())
	} else {
		r.destValue.Elem().Set(destValue)
	}
}

func (r *Collector) Clone() *Collector {
	slicePtrValue := reflect.New(r.view.Schema.SliceType())
	dest := reflect.MakeSlice(r.view.Schema.SliceType(), 0, 1)
	slicePtrValue.Elem().Set(dest)
	return &Collector{
		parent:          r.parent,
		destValue:       slicePtrValue,
		appender:        r.slice.Appender(xunsafe.ValuePointer(&slicePtrValue)),
		valuePosition:   r.valuePosition,
		types:           r.types,
		relation:        r.relation,
		values:          r.values,
		slice:           r.slice,
		view:            r.view,
		relations:       r.relations,
		dataSync:        r.dataSync,
		wg:              r.wg,
		readAll:         r.readAll,
		wgDelta:         r.wgDelta,
		indexCounter:    r.indexCounter,
		manyCounter:     r.manyCounter,
		codecSlice:      r.codecSlice,
		codecSliceDest:  r.codecSliceDest,
		codecAppender:   r.codecAppender,
		viewMetaHandler: r.viewMetaHandler,
	}
}

func (r *Collector) Lock() *sync.Mutex {
	if r.parent == nil {
		return &r.mutex
	}
	return &r.parent.mutex
}

// Resolve resolved unmapped column
func (r *Collector) Resolve(column io.Column) func(ptr unsafe.Pointer) interface{} {
	buffer, ok := r.values[column.Name()]
	if !ok {
		localSlice := make([]interface{}, 0)
		buffer = &localSlice
		r.values[column.Name()] = buffer
	}

	scanType := column.ScanType()
	kind := scanType.Kind()
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

// parentValuesPositions returns positions in the parent main slice by given column name
// After first use, it is not possible to index new resolved column indexes by Resolve method
func (r *Collector) parentValuesPositions(ns string, columnName string) map[interface{}][]int {
	columnValues, ok := r.parent.valuePosition[ns]
	if !ok {
		columnValues = map[string]map[interface{}][]int{}
		r.parent.valuePosition[ns] = columnValues
	}
	result, ok := columnValues[columnName]
	if !ok || len(result) == 0 {
		r.indexParentPositions(ns, columnName)
		result = columnValues[columnName]
	}
	return result
}

// NewCollector creates a collector
func NewCollector(slice *xunsafe.Slice, view *View, dest interface{}, viewMetaHandler viewSummaryHandlerFn, readAll bool) *Collector {
	ensuredDest := ensureDest(dest, view)
	wg := sync.WaitGroup{}
	wg.Add(1)
	return &Collector{
		destValue:       reflect.ValueOf(ensuredDest),
		valuePosition:   make(map[string]map[string]map[interface{}][]int),
		appender:        slice.Appender(xunsafe.AsPointer(ensuredDest)),
		slice:           slice,
		view:            view,
		types:           make(map[string]*xunsafe.Type),
		values:          make(map[string]*[]interface{}),
		readAll:         readAll,
		wg:              &wg,
		dataSync:        handler.DataSync{},
		wgDelta:         1,
		viewMetaHandler: viewMetaHandler,
	}
}

func ensureDest(dest interface{}, view *View) interface{} {
	if _, ok := dest.(*interface{}); ok {
		rValue := reflect.New(view.Schema.SliceType())
		rValue.Elem().Set(reflect.MakeSlice(view.Schema.SliceType(), 0, 1))
		return rValue.Elem()
	}
	return dest
}

// Visitor creates visitor function
func (r *Collector) Visitor(ctx context.Context) VisitorFn {
	relation := r.relation
	visitorRelations := RelationsSlice(r.view.With).PopulateWithVisitor()
	for _, rel := range visitorRelations {
		for _, item := range rel.On {
			if _, ok := r.valuePosition[item.Namespace]; !ok {
				r.valuePosition[item.Namespace] = map[string]map[interface{}][]int{}
			}
			r.valuePosition[item.Namespace][item.Column] = map[interface{}][]int{}
		}
	}
	visitors := make([]VisitorFn, 1)
	visitors[0] = r.valueIndexer(ctx, visitorRelations)

	if relation != nil && (r.parent == nil || !r.parent.ReadAll()) {
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

func (r *Collector) valueIndexer(ctx context.Context, visitorRelations []*Relation) func(value interface{}) error {
	distinctRelations := make([]*Relation, 0)
	presenceMap := map[string]map[string]bool{}

	for i := range visitorRelations {
		for _, item := range visitorRelations[i].On {
			if _, ok := presenceMap[item.Namespace]; !ok {
				presenceMap[item.Namespace] = map[string]bool{}
			}
			if _, ok := presenceMap[item.Namespace][item.Column]; ok {
				continue
			}
			distinctRelations = append(distinctRelations, visitorRelations[i])
			presenceMap[item.Namespace][item.Column] = true
		}
	}

	return func(value interface{}) error {
		ptr := xunsafe.AsPointer(value)
		for _, rel := range distinctRelations {
			for _, link := range rel.On {
				if field := link.xField; field != nil {
					fieldValue := field.Value(ptr)
					r.indexValueByRel(fieldValue, rel, r.indexCounter)
				}
			}
		}
		r.indexCounter++
		if r.view._codec != nil {
			r.appender.Append(value)
		}

		return nil
	}
}

func (r *Collector) indexValueByRel(fieldValue interface{}, rel *Relation, counter int) {
	switch actual := fieldValue.(type) {
	case []int:
		for _, v := range actual {
			r.indexValueToPosition(rel, v, counter)
		}
	case []*int64:
		for _, v := range actual {
			r.indexValueToPosition(rel, int(*v), counter)
		}
	case []int64:
		for _, v := range actual {
			r.indexValueToPosition(rel, int(v), counter)
		}
	case int32:
		r.indexValueToPosition(rel, int(actual), counter)

	case *int64:
		r.indexValueToPosition(rel, int(*actual), counter)
	case []string:
		for _, v := range actual {
			r.indexValueToPosition(rel, v, counter)
		}
	default:
		r.indexValueToPosition(rel, io.NormalizeKey(fieldValue), counter)
	}
}

// 6c0d0
func (r *Collector) indexValueToPosition(rel *Relation, fieldValue interface{}, counter int) {

	for _, item := range rel.On {
		columnValues, ok := r.valuePosition[item.Namespace]
		if !ok {
			columnValues = map[string]map[interface{}][]int{}
			r.valuePosition[item.Namespace] = columnValues
		}

		_, ok = columnValues[item.Column][fieldValue]
		if !ok {
			columnValues[item.Column][fieldValue] = []int{counter}
		} else {
			columnValues[item.Column][fieldValue] = append(columnValues[item.Column][fieldValue], counter)
		}
	}
}

func (r *Collector) visitorOne(relation *Relation) func(value interface{}) error {
	links := relation.Of.On
	holderField := relation.holderField
	dest := r.parent.Dest()
	destPtr := xunsafe.AsPointer(dest)
	var aKey interface{}

	return func(owner interface{}) error {
		for j, link := range links {
			if link.xField == nil {
				return fmt.Errorf("link %v field %v is not found", relation.Name, link.Column)
			}
			aKey = link.xField.Interface(xunsafe.AsPointer(owner))
			aKey = io.NormalizeKey(aKey)

			parentLink := relation.On[j]
			valuePosition := r.parentValuesPositions(parentLink.Namespace, parentLink.Column)
			positions, ok := valuePosition[aKey]
			if !ok {
				return nil
			}
			for _, index := range positions {
				item := r.parent.slice.ValuePointerAt(destPtr, index)
				holderField.SetValue(xunsafe.AsPointer(item), owner)
			}
		}
		return nil
	}

}

func (r *Collector) Parent() *Collector {
	return r.parent
}

func (r *Collector) DataSync() handler.DataSync {
	return r.dataSync
}

func (r *Collector) ParentRow(relation *Relation) func(value interface{}) (interface{}, error) {
	if relation == nil {
		return nil
	}
	links := relation.Of.On
	var xType *xunsafe.Type
	var values *[]interface{}
	dest := r.parent.Dest()
	destPtr := xunsafe.AsPointer(dest)
	if len(links) == 1 {
		keyField := links[0].xField
		xType = r.types[links[0].Column]
		column := relation.On[0].Column
		namespace := relation.On[0].Namespace
		return func(child interface{}) (interface{}, error) {
			var key interface{}
			if keyField != nil {
				key = keyField.Interface(xunsafe.AsPointer(child))
			} else {
				key = xType.Deref((*values)[r.manyCounter])
			}
			valuePosition := r.parentValuesPositions(namespace, column)
			key = io.NormalizeKey(key)
			positions, ok := valuePosition[key]
			if !ok {
				return nil, fmt.Errorf(`key "%v" is not found`, key)
			}
			if len(positions) > 1 {
				return nil, fmt.Errorf(`key "%v" has more than one value`, key)
			}
			parentItem := r.parent.slice.ValuePointerAt(destPtr, positions[0])
			return parentItem, nil
		}
	}

	return func(child interface{}) (interface{}, error) {
		var key interface{}
		var parentPosition int
		for i, link := range links {
			keyField := link.xField
			if keyField == nil && xType == nil {
				xType = r.types[link.Column]
				values = r.values[link.Column]
			}

			if keyField != nil {
				key = keyField.Interface(xunsafe.AsPointer(child))
			} else {
				key = xType.Deref((*values)[r.manyCounter])
			}
			valuePosition := r.parentValuesPositions(relation.On[i].Namespace, relation.On[i].Column)
			key = io.NormalizeKey(key)
			positions, ok := valuePosition[key]
			if !ok {
				return nil, fmt.Errorf(`key "%v" is not found`, key)
			}
			if len(positions) > 1 {
				return nil, fmt.Errorf(`key "%v" has more than one value`, key)
			}
			parentPosition = positions[0]
		}
		parentItem := r.parent.slice.ValuePointerAt(destPtr, parentPosition)
		return parentItem, nil
	}
}

func (r *Collector) visitorMany(relation *Relation) func(value interface{}) error {
	links := relation.Of.On
	holderField := relation.holderField
	var xType *xunsafe.Type
	var values *[]interface{}
	dest := r.parent.Dest()
	destPtr := xunsafe.AsPointer(dest)

	return func(owner interface{}) error {
		var key interface{}
		for i, link := range links {
			keyField := link.xField
			if keyField == nil && xType == nil {
				xType = r.types[link.Column]
				values = r.values[link.Column]
			}

			if keyField != nil {
				key = keyField.Interface(xunsafe.AsPointer(owner))
			} else {
				key = xType.Deref((*values)[r.manyCounter])
				r.manyCounter++
			}
			valuePosition := r.parentValuesPositions(relation.On[i].Namespace, relation.On[i].Column)
			key = io.NormalizeKey(key)
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
		}
		return nil
	}
}

// NewItem creates and return item provider
func (r *Collector) NewItem() func() interface{} {
	if r.view._codec == nil {
		return func() interface{} {
			return r.appender.Add()
		}
	}

	codecSlice := reflect.SliceOf(r.view.DatabaseType())
	r.codecSlice = xunsafe.NewSlice(codecSlice)
	codecSliceDest := reflect.New(codecSlice)
	r.codecSliceDest = codecSliceDest.Interface()
	r.codecAppender = r.codecSlice.Appender(unsafe.Pointer(codecSliceDest.Pointer()))

	//Adding elements to slice using xunsafe is 2.5x faster than reflect.New
	return func() interface{} {
		return r.codecAppender.Add()
	}
}

func (r *Collector) indexParentPositions(ns, name string) {
	if r.parent == nil {
		return
	}

	r.parent.indexPositions(ns, name)
}

func (r *Collector) indexPositions(ns, name string) {
	values := r.values[name]
	if values == nil {
		return
	}

	xType := r.types[name]
	columnValues, ok := r.valuePosition[ns]
	if !ok {
		columnValues = map[string]map[interface{}][]int{}
		r.valuePosition[ns] = columnValues
	}

	if _, ok := columnValues[name]; !ok {
		columnValues[name] = map[interface{}][]int{}
	}
	for position, v := range *values {
		if v == nil {
			continue
		}

		val := xType.Deref(v)
		val = io.NormalizeKey(val)
		_, ok := columnValues[name][val]
		if !ok {
			columnValues[name][val] = make([]int, 0)
		}

		columnValues[name][val] = append(columnValues[name][val], position)
	}
}

// Relations creates and register new Collector for each Relation present in the Template.Columns if View allows use Template.Columns
func (r *Collector) Relations(selector *Statelet) ([]*Collector, error) {
	result := make([]*Collector, len(r.view.With))

	counter := 0

	for i, rel := range r.view.With {
		if selector != nil && len(selector.Columns) > 0 && !selector.Has(r.view.With[i].Holder) {
			continue
		}
		lock := &sync.RWMutex{}
		lock.Lock()
		r.dataSync[rel.Holder] = lock

		destPtr := reflect.New(r.view.With[i].Of.View.Schema.SliceType())
		dest := reflect.MakeSlice(r.view.With[i].Of.View.Schema.SliceType(), 0, 1)
		destPtr.Elem().Set(dest)
		slice := rel.Of.View.Schema.Slice()
		wg := sync.WaitGroup{}

		delta := 0
		if !r.ReadAll() {
			delta = 1
		}
		wg.Add(delta)

		aHandler, err := r.ViewMetaHandler(r.view.With[i])
		if err != nil {
			return nil, err
		}
		result[counter] = &Collector{
			parent:          r,
			viewMetaHandler: aHandler,
			destValue:       destPtr,
			dataSync:        handler.DataSync{},
			appender:        slice.Appender(xunsafe.ValuePointer(&destPtr)),
			valuePosition:   make(map[string]map[string]map[interface{}][]int),
			types:           make(map[string]*xunsafe.Type),
			values:          make(map[string]*[]interface{}),
			slice:           slice,
			view:            &r.view.With[i].Of.View,
			relation:        r.view.With[i],
			readAll:         r.view.With[i].Of.MatchStrategy.ReadAll(),
			wg:              &wg,
			wgDelta:         delta,
		}
		counter++
	}

	r.relations = result[:counter]
	return result[:counter], nil
}

func (r *Collector) ViewMetaHandler(rel *Relation) (func(viewMeta interface{}) error, error) {
	templateMeta := rel.Of.View.Template.Summary
	if templateMeta == nil {
		return func(viewMeta interface{}) error {
			return nil
		}, nil
	}
	//TODO refactor it so the multi relation fields can be used here

	fieldCaseFormat := text.DetectCaseFormat(rel.Of.On[0].Field)
	childMetaFieldName := fieldCaseFormat.Format(rel.Of.On[0].Field, text.CaseFormatUpperCamel)
	metaChildKeyField := xunsafe.FieldByName(templateMeta.Schema.Type(), childMetaFieldName)
	if metaChildKeyField == nil {
		return nil, fmt.Errorf("not found field %v at %v", childMetaFieldName, templateMeta.Schema.Type().String())
	}

	metaParentHolderField := xunsafe.FieldByName(r.view.Schema.CompType(), templateMeta.Name)
	if metaParentHolderField == nil {
		return nil, fmt.Errorf("not found holder field %v at %v", templateMeta.Name, templateMeta.Schema.Type().String())
	}

	var valuesPosition map[interface{}][]int
	return func(viewMeta interface{}) error {
		for _, item := range rel.On {
			if valuesPosition == nil {
				if r.valuePosition[item.Namespace] == nil {
					r.valuePosition[item.Namespace] = map[string]map[interface{}][]int{}
				}
				valuesPosition = r.valuePosition[item.Namespace][item.Column]
			}

			viewMetaPtr := xunsafe.AsPointer(viewMeta)
			if viewMetaPtr == nil {
				return nil
			}

			value := io.NormalizeKey(metaChildKeyField.Value(viewMetaPtr))
			positions, ok := valuesPosition[value]
			if !ok {
				return nil
			}

			slicePtr := xunsafe.AsPointer(r.DestPtr())
			for _, position := range positions {
				ownerPtr := xunsafe.AsPointer(r.slice.ValuePointerAt(slicePtr, position))
				metaParentHolderField.SetValue(ownerPtr, viewMeta)
			}
		}

		return nil
	}, nil
}

// View returns View assigned to the Collector
func (r *Collector) View() *View {
	return r.view
}

// Project returns collector slice ptr
func (r *Collector) DestPtr() interface{} {
	return r.destValue.Interface()
}

// Project returns collector slice
func (r *Collector) Dest() interface{} {
	return r.destValue.Elem().Interface()
}

// ReadAll if Collector uses readAll flag, it means that his Relations can fetch all data View in the same time, (no matching parent data)
// Later on it will be merged with the parent Collector
func (r *Collector) ReadAll() bool {
	return r.readAll
}

func (r *Collector) Unlock() {
	if r.parent == nil {
		return
	}
	if lock, ok := r.parent.dataSync[r.relation.Holder]; ok {
		delete(r.parent.dataSync, r.relation.Holder)
		lock.Unlock()
	}
}

// MergeData merges View with Collectors produced via Relations
// It is sufficient to call it on the most locators Collector to produce result
func (r *Collector) MergeData() {
	for i := range r.relations {
		r.relations[i].MergeData()
	}

	if r.parent == nil || !r.ReadAll() {
		return
	}

	r.mergeToParent()
}

func (r *Collector) mergeToParent() {
	links := r.relation.Of.On

	for i, link := range links {
		valuePositions := r.parentValuesPositions(r.relation.On[i].Namespace, r.relation.On[i].Column)
		destPtr := xunsafe.AsPointer(r.DestPtr())
		holderField := r.relation.holderField
		parentSlice := r.parent.slice
		parentDestPtr := xunsafe.AsPointer(r.parent.DestPtr())

		field := link.xField
		for i := 0; i < r.slice.Len(destPtr); i++ {
			value := r.slice.ValuePointerAt(destPtr, i)
			key := io.NormalizeKey(field.Value(xunsafe.AsPointer(value)))
			positions, ok := valuePositions[key]
			if !ok {
				continue
			}

			for _, position := range positions {
				parentValue := parentSlice.ValuePointerAt(parentDestPtr, position)
				if r.relation.Cardinality == state.One {
					at := r.slice.ValuePointerAt(destPtr, i)
					holderField.SetValue(xunsafe.AsPointer(parentValue), at)
				} else if r.relation.Cardinality == state.Many {
					r.Lock().Lock()
					appender := r.slice.Appender(holderField.ValuePointer(xunsafe.AsPointer(parentValue)))
					appender.Append(value)
					r.Lock().Unlock()
					r.view.Logger.ObjectReconciling(r.Dest(), value, parentValue, position)
				}
			}
		}
	}
}

// ParentPlaceholders if Collector doesn't support parallel fetching and has a locators, it will return a parent _field values and column name
// that the relation was created from, otherwise empty slice and empty string
// i.e. if locators Collector collects Employee{AccountId: int}, Column.Name is account_id and Collector collects Account
// it will extract and return all the AccountId that were accumulated and account_id
func (r *Collector) ParentPlaceholders() ([]interface{}, []string) {
	if r.parent == nil || r.ReadAll() {
		return []interface{}{}, nil
	}
	destPtr := xunsafe.AsPointer(r.parent.DestPtr())
	sliceLen := r.parent.slice.Len(destPtr)
	result := make([]interface{}, 0)
	var unique = make(map[any]bool)
outer:
	for i := 0; i < sliceLen; i++ {
		parent := r.parent.slice.ValuePointerAt(destPtr, i)
		for k, link := range r.relation.On {
			field := link.xField
			if field != nil {
				fieldValue := field.Value(xunsafe.AsPointer(parent))

				switch actual := fieldValue.(type) {
				case []*int64:

					for j := range actual {
						if _, ok := unique[int(*actual[j])]; ok {
							continue
						}
						unique[int(*actual[j])] = true
						result = append(result, int(*actual[j]))
					}
				case []int64:

					for j := range actual {
						if _, ok := unique[int(actual[j])]; ok {
							continue
						}
						unique[int(actual[j])] = true
						result = append(result, int(actual[j]))
					}
				case []int:

					for j := range actual {
						if _, ok := unique[actual[j]]; ok {
							continue
						}
						unique[actual[j]] = true
						result = append(result, actual[j])
					}
				case []string:

					for j := range actual {
						if _, ok := unique[actual[j]]; ok {
							continue
						}
						unique[actual[j]] = true
						result = append(result, actual[j])
					}
				default:
					result = append(result, fieldValue)
				}
				continue
			}

			positions := r.parentValuesPositions(r.relation.On[k].Namespace, r.relation.On[k].Column)
			result := make([]interface{}, len(positions))
			counter := 0
			for key := range positions {
				result[counter] = key
				counter++
			}
			continue outer
		}
	}
	return result, r.relation.Of.On.InColumnExpression()
}

func (r *Collector) WaitIfNeeded() {
	//if r.readAll {
	//	return
	//}
	if r.parent != nil {
		r.parent.wg.Wait()
	}
}

func (r *Collector) Fetched() {
	r.createTreeIfNeeded()
	if r.wgDelta > 0 {
		r.wg.Done()
		r.wgDelta--
	}
}

func (r *Collector) Len() int {
	if r.DestPtr() != nil {
		return (*reflect.SliceHeader)(xunsafe.AsPointer(r.DestPtr())).Len
	}
	return 0
}

func (r *Collector) Slice() (unsafe.Pointer, *xunsafe.Slice) {
	return xunsafe.AsPointer(r.DestPtr()), r.slice
}

func (r *Collector) Relation() *Relation {
	return r.relation
}

func (r *Collector) AddMeta(row interface{}) error {
	return r.viewMetaHandler(row)
}

func (r *Collector) createTreeIfNeeded() {
	if r.view.SelfReference == nil {
		return
	}

	aTree := BuildTree(r.view.Schema.Type(), r.view.Schema.Slice(), r.DestPtr(), r.view.SelfReference, r.view.CaseFormat)
	if aTree != nil {
		r.SetDest(aTree)
	}
}

func (r *Collector) OnSkip(_ []interface{}) error {
	sliceLen := r.slice.Len(xunsafe.AsPointer(r.DestPtr()))
	if sliceLen == 0 {
		return nil
	}

	return r.appender.Trunc(sliceLen - 1)
}

type NodeIndex map[interface{}]map[interface{}]bool

func (i NodeIndex) Get(id interface{}) map[interface{}]bool {
	index, ok := i[id]
	if !ok {
		index = map[interface{}]bool{}
		i[id] = index
	}

	return index
}

func BuildTree(schemaType reflect.Type, slice *xunsafe.Slice, nodes interface{}, reference *SelfReference, caseFormat text.CaseFormat) interface{} {
	nodesPtr := xunsafe.AsPointer(nodes)
	if nodesPtr == nil {
		return nodes
	}

	idField := xunsafe.FieldByName(schemaType, caseFormat.Format(reference.Child, text.CaseFormatUpperCamel))
	parentField := xunsafe.FieldByName(schemaType, caseFormat.Format(reference.Parent, text.CaseFormatUpperCamel))
	holderField := xunsafe.FieldByName(schemaType, reference.Holder)
	holderSlice := xunsafe.NewSlice(holderField.Type)

	index := map[interface{}]int{}
	nodesLen := slice.Len(nodesPtr)
	for i := 0; i < nodesLen; i++ {
		index[keyAt(idField, slice, nodesPtr, i)] = i // first I am indexing nodes by "ID"
	}

	indexes := NodeIndex{}
	for i := 0; i < nodesLen; i++ {
		node := slice.ValueAt(nodesPtr, i)
		nodeParentIndex, ok := index[keyAt(parentField, slice, nodesPtr, i)]

		for ok { //then I am appending item to the parent, and parent to his parent and so on...,
			parentIndex := index[keyAt(parentField, slice, nodesPtr, nodeParentIndex)]
			parent := slice.ValuePointerAt(nodesPtr, nodeParentIndex)
			nodeIndex := indexes.Get(keyAt(idField, slice, nodesPtr, parentIndex))
			nodeId := key(idField, node)

			if !nodeIndex[nodeId] { // only if item was not already added to the parent. If item was already added, it means that this node and his parents were already updated.
				currentNode := slice.ValuePointerAt(nodesPtr, index[key(idField, node)])
				parentPtr := xunsafe.AsPointer(parent)

				asIfaceSlice, isIfaceSlice := holderField.Value(parentPtr).([]interface{})
				if isIfaceSlice {
					asIfaceSlice = append(asIfaceSlice, currentNode)
					holderField.SetValue(parentPtr, asIfaceSlice)
				} else {
					holder := holderField.ValuePointer(parentPtr)
					holderSlice.Appender(holder).Append(currentNode)
				}

				nodeIndex[nodeId] = true
				node = parent
				nodeParentIndex, ok = index[key(parentField, parent)]
				continue
			}
			break
		}
	}

	result := reflect.New(slice.Type)
	resultAppender := slice.Appender(unsafe.Pointer(result.Pointer()))
	for i := 0; i < nodesLen; i++ { // then I am collecting all Nodes without parents
		ownerParentFieldValue := keyAt(parentField, slice, nodesPtr, i)
		if ownerParentFieldValue == nil || xunsafe.AsPointer(ownerParentFieldValue) == nil {
			resultAppender.Append(slice.ValuePointerAt(nodesPtr, i))
			continue
		}

		_, ok := index[ownerParentFieldValue]
		if !ok {
			resultAppender.Append(slice.ValuePointerAt(nodesPtr, i))
		}
	}

	return result.Interface()
}

func key(field *xunsafe.Field, node interface{}) interface{} {
	return io.NormalizeKey(field.Value(xunsafe.AsPointer(node)))
}

func keyAt(field *xunsafe.Field, slice *xunsafe.Slice, nodesPtr unsafe.Pointer, i int) interface{} {
	return io.NormalizeKey(field.Value(xunsafe.AsPointer(slice.ValuePointerAt(nodesPtr, i))))
}
