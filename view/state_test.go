package view

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/view/state/predicate"
)

func TestStateletCloneForSummaryConcurrentFilters(t *testing.T) {
	statelet := NewStatelet()
	filter := &predicate.Filter{Name: "active"}

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		for i := 0; i < 1000; i++ {
			statelet.AppendFilters(predicate.Filters{filter})
			statelet.ClearFilters()
		}
	}()

	go func() {
		defer waitGroup.Done()
		for i := 0; i < 1000; i++ {
			clone := statelet.CloneForSummary()
			if clone == statelet {
				t.Errorf("CloneForSummary() returned the original statelet")
				return
			}
		}
	}()

	waitGroup.Wait()
}

// TestStateletCloneForSummaryConcurrentColumnNames reproduces the production
// fatal "concurrent map iteration and map write" on Statelet._columnNames.
// CloneForSummary ranges the map while Add writes it. Surviving the 3s stress
// window is the assertion that the race is gone.
func TestStateletCloneForSummaryConcurrentColumnNames(t *testing.T) {
	deadline := time.Now().Add(3 * time.Second)
	statelet := NewStatelet()
	var next atomic.Uint64
	var waitGroup sync.WaitGroup
	waitGroup.Add(5)

	run := func(fn func()) {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			fn()
		}
	}

	// One writer so we do not crash on concurrent Add lookups first.
	go run(func() {
		statelet.Add(fmt.Sprintf("Field%d", next.Add(1)), true)
	})
	for worker := 0; worker < 4; worker++ {
		go run(func() {
			_ = statelet.CloneForSummary()
		})
	}
	waitGroup.Wait()
}
