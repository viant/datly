package state

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func uniqueType(n uint64) reflect.Type {
	return reflect.StructOf([]reflect.StructField{{
		Name: "F",
		Type: reflect.TypeOf(0),
		Tag:  reflect.StructTag(fmt.Sprintf(`json:"f%d"`, n)),
	}})
}

// TestTypesConcurrentLookupAndPut reproduces concurrent map access on Types.types.
// Lookup reads the map (including an unlocked len) while Put writes it.
func TestTypesConcurrentLookupAndPut(t *testing.T) {
	deadline := time.Now().Add(3 * time.Second)
	registry := NewTypes()
	var next atomic.Uint64
	var waitGroup sync.WaitGroup
	waitGroup.Add(8)

	run := func(fn func()) {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			fn()
		}
	}

	for writer := 0; writer < 4; writer++ {
		go run(func() {
			registry.Put(&Type{Schema: &Schema{rType: uniqueType(next.Add(1))}})
		})
	}
	for worker := 0; worker < 4; worker++ {
		go run(func() {
			_, _ = registry.Lookup(uniqueType(next.Add(1)))
		})
	}
	waitGroup.Wait()
}
