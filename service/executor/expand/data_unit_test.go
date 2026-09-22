package expand

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDataUnitConcurrentSliceIndex reproduces concurrent map write on
// DataUnit.sliceIndex. xunsafeSlice writes the map with no lock.
func TestDataUnitConcurrentSliceIndex(t *testing.T) {
	deadline := time.Now().Add(3 * time.Second)
	unit := &DataUnit{}
	unit.ensureSliceIndex()
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
			n := int(next.Add(1)%64) + 1
			_ = unit.xunsafeSlice(reflect.ArrayOf(n, reflect.TypeOf(byte(0))))
		})
	}
	waitGroup.Wait()
}
