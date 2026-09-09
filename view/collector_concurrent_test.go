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
// iteration and write on Collector.valuePosition. Child collectors write the
// parent map while another goroutine ranges it.
func TestCollectorConcurrentParentValuePositions(t *testing.T) {
	deadline := time.Now().Add(3 * time.Second)
	parentView := &View{Schema: state.NewSchema(reflect.TypeOf([]*compositeParentRow{}))}
	parentDest := []*compositeParentRow{}
	parent := NewCollector(parentView.Schema.Slice(), parentView, &parentDest, nil, false)
	child := &Collector{parent: parent}
	var next atomic.Uint64
	var waitGroup sync.WaitGroup
	waitGroup.Add(5)

	run := func(fn func()) {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			fn()
		}
	}

	for worker := 0; worker < 5; worker++ {
		go run(func() {
			ns := fmt.Sprintf("ns%d", next.Add(1))
			_ = child.parentValuesPositions(ns, "col")
		})
	}
	waitGroup.Wait()
}
