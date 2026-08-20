package view

import (
	"sync"
	"testing"

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
