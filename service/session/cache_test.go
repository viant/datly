package session

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/view/state"
)

// TestSessionMarshalJSONConcurrentCachePut reproduces concurrent map
// iteration and write on session cache.values. MarshalJSON ranges the map
// while put writes it.
func TestSessionMarshalJSONConcurrentCachePut(t *testing.T) {
	deadline := time.Now().Add(3 * time.Second)
	s := &Session{cache: newCache()}
	var next atomic.Uint64
	var waitGroup sync.WaitGroup
	waitGroup.Add(5)

	run := func(fn func()) {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			fn()
		}
	}

	go run(func() {
		s.cache.put(&state.Parameter{Name: fmt.Sprintf("p%d", next.Add(1))}, 1)
	})
	for worker := 0; worker < 4; worker++ {
		go run(func() {
			_, _ = s.MarshalJSON()
		})
	}
	waitGroup.Wait()
}
