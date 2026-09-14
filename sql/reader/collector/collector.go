package collector

// Collector scans SQL rows into typed Go slices and reconciles parent-child
// relations via position indexes built during the scan.
//
// Usage pattern:
//
//	c := NewCollector(view, dest, readAll)
//	children := c.Relations(nil)
//	// ... scan rows, calling visitor for each row ...
//	c.Fetched()
//	_ = c.MergeData()

import (
	"reflect"
	"sync"

	"github.com/google/uuid"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
)

// VisitorFn is called once per scanned row. It indexes values and/or sets
// parent-struct fields for One/Many relations.
type VisitorFn func(value interface{}) error

// Collector collects and builds a result from one View fetched from the
// database. When a View or any of its With relations supports parallel
// fetching (ReadAll), call MergeData after all views have been fetched.
type Collector struct {
	Id                     string
	mutex                  sync.Mutex
	indexMutex             *sync.Mutex
	parent                 *Collector
	destValue              reflect.Value
	appender               *xunsafe.Appender
	valuePosition          map[string]map[string]map[interface{}][]int // ns -> col -> val -> positions
	compositeValuePosition map[string]map[compositeKey][]int
	preparedValuePosition  map[string]map[string]bool
	preparedComposite      map[string]bool
	types                  map[string]*xunsafe.Type
	relation               *Relation
	dataSync               *xhandler.DataSync
	values                 map[string]*[]interface{} // buffer for Resolve'd (unmapped) columns

	slice     *xunsafe.Slice
	view      *View
	relations []*Collector

	wg      *sync.WaitGroup
	readAll bool
	wgDelta int

	indexCounter int
	provenance   bool
	rowEvidence  []*rowEvidence
}

// NewCollector creates a root Collector for view, writing into dest.
// dest must be a pointer to a slice (or *interface{} for dynamic dispatch).
func NewCollector(view *View, dest interface{}, readAll bool) *Collector {
	if view == nil {
		return nil
	}
	slice := view.Schema.Slice()
	ensuredDest := ensureDest(dest, view)
	wg := sync.WaitGroup{}
	wg.Add(1)
	return &Collector{
		Id:                     uuid.New().String(),
		indexMutex:             &sync.Mutex{},
		destValue:              reflect.ValueOf(ensuredDest),
		valuePosition:          make(map[string]map[string]map[interface{}][]int),
		compositeValuePosition: make(map[string]map[compositeKey][]int),
		preparedValuePosition:  make(map[string]map[string]bool),
		preparedComposite:      make(map[string]bool),
		appender:               slice.Appender(xunsafe.AsPointer(ensuredDest)),
		slice:                  slice,
		view:                   view,
		types:                  make(map[string]*xunsafe.Type),
		values:                 make(map[string]*[]interface{}),
		readAll:                readAll,
		wg:                     &wg,
		dataSync:               xhandler.NewDataSync(),
		wgDelta:                1,
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
