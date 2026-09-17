package collector

import (
	"reflect"
	"strings"
	"sync"

	"github.com/google/uuid"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
)

// Statelet carries optional column-selection state used to prune relations.
type Statelet struct {
	Columns []string
}

// Has reports whether holder appears in the selected columns list.
func (s *Statelet) Has(holder string) bool {
	if s == nil || len(s.Columns) == 0 {
		return false
	}
	for _, col := range s.Columns {
		if strings.EqualFold(strings.TrimSpace(col), strings.TrimSpace(holder)) {
			return true
		}
	}
	return false
}

// Relations creates child Collectors for each Relation on the view.
// selector may be nil (no column pruning).
func (r *Collector) Relations(selector *Statelet) []*Collector {
	result := make([]*Collector, len(r.view.Relations))
	counter := 0

	for i, rel := range r.view.Relations {
		if rel == nil || rel.Of == nil || rel.Of.View == nil {
			continue
		}
		if rel.IsOutput() {
			continue
		}
		if selector != nil && len(selector.Columns) > 0 && !selector.Has(r.view.Relations[i].Holder) {
			continue
		}
		r.dataSync.Put(rel.Holder)

		destPtr := reflect.New(r.view.Relations[i].Of.View.Schema.SliceType())
		dest := reflect.MakeSlice(r.view.Relations[i].Of.View.Schema.SliceType(), 0, 1)
		destPtr.Elem().Set(dest)
		slice := rel.Of.View.Schema.Slice()
		wg := sync.WaitGroup{}

		delta := 0
		if !r.ReadAll() {
			delta = 1
		}
		wg.Add(delta)

		result[counter] = &Collector{
			Id:                     uuid.New().String(),
			indexMutex:             &sync.Mutex{},
			parent:                 r,
			destValue:              destPtr,
			dataSync:               xhandler.NewDataSync(),
			appender:               slice.Appender(xunsafe.ValuePointer(&destPtr)),
			valuePosition:          make(map[relationIndexKey]map[interface{}][]int),
			compositeValuePosition: make(map[string]map[compositeKey][]int),
			preparedValuePosition:  make(map[relationIndexKey]bool),
			preparedComposite:      make(map[string]bool),
			types:                  make(map[string]*xunsafe.Type),
			values:                 make(map[string]*[]interface{}),
			slice:                  slice,
			view:                   r.view.Relations[i].Of.View,
			relation:               r.view.Relations[i],
			readAll:                r.view.Relations[i].Of.MatchStrategy.ReadAll(),
			wg:                     &wg,
			wgDelta:                delta,
			provenance:             r.provenance,
		}
		counter++
	}

	r.relations = result[:counter]
	return result[:counter]
}
