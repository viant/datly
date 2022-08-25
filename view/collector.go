package view

import (
	"context"
	"github.com/viant/sqlx/io"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"unsafe"
)

//Visitor represents visitor function
type Visitor func(value interface{}) error

//Collector collects and build result from view fetched from Database
//If View or any of the View.With MatchStrategy support Parallel fetching, it is important to call MergeData
//when all needed view was fetched
type Collector struct {
	mutex  sync.Mutex
	parent *Collector

	dest          interface{}
	appender      *xunsafe.Appender
	valuePosition map[string]map[interface{}][]int //stores positions in main slice, based on _field name, indexed by _field value.
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

	indexCounter    int
	manyCounter     int
	codecSlice      *xunsafe.Slice
	codecSliceDest  interface{}
	codecAppender   *xunsafe.Appender
	viewMetaHandler viewMetaHandlerFn
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
	scanType = remapScanType(scanType, column.DatabaseTypeName())
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
func NewCollector(slice *xunsafe.Slice, view *View, dest interface{}, viewMetaHandler viewMetaHandlerFn, supportParallel bool) *Collector {
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
		viewMetaHandler: viewMetaHandler,
	}
}

func ensureDest(dest interface{}, view *View) interface{} {
	if _, ok := dest.(*interface{}); ok {
		return reflect.MakeSlice(view.Schema.SliceType(), 0, 1).Interface()
	}
	return dest
}

//Visitor creates visitor function
func (r *Collector) Visitor(ctx context.Context) Visitor {
	relation := r.relation
	visitorRelations := RelationsSlice(r.view.With).PopulateWithVisitor()
	for _, rel := range visitorRelations {
		r.valuePosition[rel.Column] = map[interface{}][]int{}
	}

	visitors := make([]Visitor, 1)
	visitors[0] = r.valueIndexer(ctx, visitorRelations)

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

func (r *Collector) valueIndexer(ctx context.Context, visitorRelations []*Relation) func(value interface{}) error {
	distinctRelations := make([]*Relation, 0)
	presenceMap := map[string]bool{}

	for i := range visitorRelations {
		if _, ok := presenceMap[visitorRelations[i].Column]; ok {
			continue
		}
		distinctRelations = append(distinctRelations, visitorRelations[i])
		presenceMap[visitorRelations[i].Column] = true
	}

	return func(value interface{}) error {
		ptr := xunsafe.AsPointer(value)
		for _, rel := range distinctRelations {
			fieldValue := rel.columnField.Value(ptr)
			r.indexValueByRel(fieldValue, rel, r.indexCounter)
		}

		r.indexCounter++
		if r.view.codec != nil {
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
		r.indexValueToPosition(rel, normalizeKey(fieldValue), counter)
	}
}

func (r *Collector) indexValueToPosition(rel *Relation, fieldValue interface{}, counter int) {
	_, ok := r.valuePosition[rel.Column][fieldValue]
	if !ok {
		r.valuePosition[rel.Column][fieldValue] = []int{counter}
	} else {
		r.valuePosition[rel.Column][fieldValue] = append(r.valuePosition[rel.Column][fieldValue], counter)
	}
}

func (r *Collector) visitorOne(relation *Relation) func(value interface{}) error {
	keyField := relation.Of._field
	holderField := relation.holderField
	dest := r.parent.Dest()
	destPtr := xunsafe.AsPointer(dest)
	var key interface{}

	return func(owner interface{}) error {
		key = keyField.Interface(xunsafe.AsPointer(owner))
		key = normalizeKey(key)
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
	keyField := relation.Of._field
	holderField := relation.holderField
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
			key = xType.Deref((*values)[r.manyCounter])
			r.manyCounter++
		}

		valuePosition := r.parentValuesPositions(relation.Column)

		key = normalizeKey(key)
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
func (r *Collector) NewItem() func() interface{} {
	if r.view.codec == nil {
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

func (r *Collector) indexParentPositions(name string) {
	if r.parent == nil {
		return
	}

	r.parent.indexPositions(name)
}

func (r *Collector) indexPositions(name string) {
	values := r.values[name]
	if values == nil {
		return
	}

	xType := r.types[name]
	r.valuePosition[name] = map[interface{}][]int{}
	for position, v := range *values {
		if v == nil {
			continue
		}

		val := xType.Deref(v)
		val = normalizeKey(val)
		_, ok := r.valuePosition[name][val]
		if !ok {
			r.valuePosition[name][val] = make([]int, 0)
		}

		r.valuePosition[name][val] = append(r.valuePosition[name][val], position)
	}
}

//Relations creates and register new Collector for each Relation present in the Selector.Columns if View allows use Selector.Columns
func (r *Collector) Relations(selector *Selector) []*Collector {
	result := make([]*Collector, len(r.view.With))

	counter := 0
	for i := range r.view.With {
		if selector != nil && len(selector.Columns) > 0 && !selector.Has(r.view.With[i].Holder) {
			continue
		}

		dest := reflect.MakeSlice(r.view.With[i].Of.View.Schema.SliceType(), 0, 1).Interface()
		slice := r.view.With[i].Of.View.Schema.Slice()
		wg := sync.WaitGroup{}

		delta := 0
		if !r.SupportsParallel() {
			delta = 1
		}
		wg.Add(delta)

		result[counter] = &Collector{
			parent:          r,
			viewMetaHandler: r.ViewMetaHandler(r.view.With[i]),
			dest:            dest,
			appender:        slice.Appender(xunsafe.AsPointer(dest)),
			valuePosition:   make(map[string]map[interface{}][]int),
			types:           make(map[string]*xunsafe.Type),
			values:          make(map[string]*[]interface{}),
			slice:           slice,
			view:            &r.view.With[i].Of.View,
			relation:        r.view.With[i],
			supportParallel: r.view.With[i].Of.MatchStrategy.SupportsParallel(),
			wg:              &wg,
			wgDelta:         delta,
		}
		counter++
	}

	r.relations = result[:counter]
	return result[:counter]
}

func (r *Collector) ViewMetaHandler(rel *Relation) func(viewMeta interface{}) error {
	templateMeta := rel.Of.View.Template.Meta
	if templateMeta == nil {
		return func(viewMeta interface{}) error {
			return nil
		}
	}

	metaChildKeyField := xunsafe.FieldByName(templateMeta.Schema.Type(), rel.Of.View.Caser.Format(rel.Of.Column, format.CaseUpperCamel))
	metaParentHolderField := xunsafe.FieldByName(r.view.Schema.Type(), rel.Of.View.Caser.Format(templateMeta.Name, format.CaseUpperCamel))
	xType := xunsafe.NewType(metaParentHolderField.Type)
	shouldDeref := xType.Kind() == reflect.Ptr
	var valuesPosition map[interface{}][]int
	return func(viewMeta interface{}) error {
		if valuesPosition == nil {
			valuesPosition = r.valuePosition[rel.Column]
		}

		value := normalizeKey(metaChildKeyField.Value(xunsafe.AsPointer(viewMeta)))
		positions, ok := valuesPosition[value]
		if !ok {
			return nil
		}

		slicePtr := xunsafe.AsPointer(r.dest)
		for _, position := range positions {
			ownerPtr := r.slice.PointerAt(slicePtr, uintptr(position))
			if shouldDeref {
				metaParentHolderField.SetValue(ownerPtr, xType.Deref(viewMeta))
			} else {
				metaParentHolderField.SetValue(ownerPtr, viewMeta)
			}
		}

		return nil
	}
}

//View returns View assigned to the Collector
func (r *Collector) View() *View {
	return r.view
}

//Dest returns collector slice
func (r *Collector) Dest() interface{} {
	return r.dest
}

//SupportsParallel if Collector supports parallelism, it means that his Relations can fetch view in the same time
//Later on it will be merged with the parent Collector
func (r *Collector) SupportsParallel() bool {
	return r.supportParallel
}

//MergeData merges view with Collectors produced via Relations
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
	field := r.relation.Of._field
	holderField := r.relation.holderField
	parentSlice := r.parent.slice
	parentDestPtr := xunsafe.AsPointer(r.parent.dest)

	for i := 0; i < r.slice.Len(destPtr); i++ {
		value := r.slice.ValuePointerAt(destPtr, i)
		key := normalizeKey(field.Value(xunsafe.AsPointer(value)))
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

//ParentPlaceholders if Collector doesn't support parallel fetching and has a Parent, it will return a parent _field values and column name
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
			case []*int64:
				for j := range actual {
					result = append(result, int(*actual[j]))
				}
			case []int64:
				for j := range actual {
					result = append(result, int(actual[j]))
				}
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
	r.createTreeIfNeeded()

	if r.wgDelta > 0 {
		r.wg.Done()
		r.wgDelta--
	}
}

func (r *Collector) Len() int {
	if r.dest != nil {
		return (*reflect.SliceHeader)(xunsafe.AsPointer(r.dest)).Len
	}
	return 0
}

func (r *Collector) Slice() (unsafe.Pointer, *xunsafe.Slice) {
	return xunsafe.AsPointer(r.dest), r.slice
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

	aTree := BuildTree(r.view.Schema.Type(), r.view.Schema.Slice(), r.dest, r.view.SelfReference, r.view.Caser)
	if aTree != nil {
		reflect.ValueOf(r.dest).Elem().Set(reflect.ValueOf(aTree).Elem())
	}
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

func BuildTree(schemaType reflect.Type, slice *xunsafe.Slice, nodes interface{}, reference *SelfReference, caser format.Case) interface{} {
	nodesPtr := xunsafe.AsPointer(nodes)
	if nodesPtr == nil {
		return nodes
	}

	idField := xunsafe.FieldByName(schemaType, caser.Format(reference.ChildColumn, format.CaseUpperCamel))
	parentField := xunsafe.FieldByName(schemaType, caser.Format(reference.ParentColumn, format.CaseUpperCamel))
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
	return normalizeKey(field.Value(xunsafe.AsPointer(node)))
}

func keyAt(field *xunsafe.Field, slice *xunsafe.Slice, nodesPtr unsafe.Pointer, i int) interface{} {
	return normalizeKey(field.Value(slice.PointerAt(nodesPtr, uintptr(i))))
}

//
//func BuildTree(nodes []*Node) []*Node {
//	if len(nodes) == 0 {
//		return []*Node{}
//	}
//
//	var parents []*Node
//	index := map[int]int{}
//
//	for i, node := range nodes {
//		index[node.ID] = i
//	}
//
//	indexes := NodeIndex{}
//
//	for i, node := range nodes {
//		nodeParentIndex, ok := index[node.ParentID]
//		if !ok {
//			parents = append(parents, nodes[i])
//			continue
//		}
//
//		for ok {
//			parent := nodes[nodeParentIndex]
//			nodeIndex := indexes.Get(parent.ID)
//			if !nodeIndex[node.ID] {
//				nodeCopy := nodes[index[node.ID]]
//				parent.Children = append(parent.Children, nodeCopy)
//			}
//
//			nodeIndex[node.ID] = true
//			node = parent
//			nodeParentIndex, ok = index[parent.ParentID]
//		}
//	}
//
//	return parents
//}
