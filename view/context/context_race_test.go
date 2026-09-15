package view

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestContextWithValueConcurrentParentAccess(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx := NewContext(base)
	type testKey struct{}

	deadline := time.Now().Add(500 * time.Millisecond)
	var next atomic.Uint64
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			WithValue(ctx, testKey{}, fmt.Sprintf("%d", next.Add(1)))
		}
	}()

	go func() {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			_, _ = ctx.Deadline()
			_ = ctx.Done()
			_ = ctx.Err()
			_ = ctx.Value(testKey{})
		}
	}()

	waitGroup.Wait()
}
