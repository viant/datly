package view

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/view/state"
)

// TestCollectorConcurrentParentValuePositions reproduces concurrent map
// iteration and write on Collector.valuePosition. Writers mutate the parent
// index while readers copy positions for a specific key.
func TestCollectorConcurrentParentValuePositions(t *testing.T) {
	deadline := time.Now().Add(3 * time.Second)
	parentView := &View{Schema: state.NewSchema(reflect.TypeOf([]*compositeParentRow{}))}
	parentDest := []*compositeParentRow{}
	parent := NewCollector(parentView.Schema.Slice(), parentView, &parentDest, nil, false)
	parent.valuePosition["ns"] = map[string]map[interface{}][]int{
		"col": {},
	}
	child := &Collector{parent: parent}
	rel := &Relation{On: Links{{Namespace: "ns", Column: "col"}}}
	var next atomic.Uint64
	var waitGroup sync.WaitGroup
	waitGroup.Add(6)

	run := func(fn func()) {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			fn()
		}
	}

	for worker := 0; worker < 3; worker++ {
		go run(func() {
			key := int(next.Add(1))
			parent.indexValueToPosition(rel, key, key%8)
		})
	}
	for worker := 0; worker < 2; worker++ {
		go run(func() {
			key := int(next.Load())
			positions := child.parentPositionsFor("ns", "col", key)
			_ = len(positions)
		})
	}
	go run(func() {
		ns := fmt.Sprintf("ns%d", next.Add(1))
		_ = child.parentPositionsFor(ns, "col", nil)
		_ = child.parentPositionKeys(ns, "col")
	})
	waitGroup.Wait()
}
