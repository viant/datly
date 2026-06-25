package view

import (
	"os"
	"sync"
	"testing"

	"github.com/viant/datly/view/state/predicate"
)

// Reproduces the prod CloneForSummary segfault: the summary goroutine clones the
// Statelet (reads Filters) while the object-query goroutine grows Filters via
// AppendFilters. Reading Filters without filtersMu is a data race; a torn slice
// header (new len + nil ptr) faults at addr=0x0.

const summaryCloneRaceIterations = 5000

// startAppendFiltersWriter runs the writer side: AppendFilters as called from
// service/reader/sql.go, repeatedly growing Filters to force reallocation.
func startAppendFiltersWriter(wg *sync.WaitGroup, statelet *Statelet) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < summaryCloneRaceIterations; i++ {
			statelet.AppendFilters(predicate.Filters{&predicate.Filter{Name: "accountID"}})
		}
	}()
}

// Fixed CloneForSummary vs AppendFilters: passes under -race.
func TestStatelet_SummaryClone_Fixed_NoRace(t *testing.T) {
	statelet := NewStatelet()

	var wg sync.WaitGroup
	startAppendFiltersWriter(&wg, statelet)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < summaryCloneRaceIterations; i++ {
			_ = statelet.CloneForSummary()
		}
	}()

	wg.Wait()
}

// Pre-fix unlocked read vs AppendFilters: fails under -race (the prod bug).
// Skipped by default; set REPRODUCE_STATELET_RACE=1 to run.
func TestStatelet_SummaryClone_Unsafe_ReproducesRace(t *testing.T) {
	if os.Getenv("REPRODUCE_STATELET_RACE") == "" {
		t.Skip("set REPRODUCE_STATELET_RACE=1 to run the intentional data-race reproduction")
	}

	statelet := NewStatelet()

	var wg sync.WaitGroup
	startAppendFiltersWriter(&wg, statelet)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < summaryCloneRaceIterations; i++ {
			_ = unsafeCloneFilters(statelet)
		}
	}()

	wg.Wait()
}

// unsafeCloneFilters reads Filters without filtersMu, mirroring the pre-fix bug.
func unsafeCloneFilters(s *Statelet) predicate.Filters {
	if len(s.Filters) == 0 {
		return nil
	}
	return append(predicate.Filters(nil), s.Filters...)
}
