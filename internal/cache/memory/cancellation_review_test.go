package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type checkedContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (c *checkedContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.checked) })
	return err
}

func TestCancellationWhileWaitingForCacheLock(t *testing.T) {
	for _, operation := range []string{"get", "put", "delete"} {
		t.Run(operation, func(t *testing.T) {
			cache, _ := New(0)
			if err := cache.Put(context.Background(), "key", []byte("original"), 0); err != nil {
				t.Fatal(err)
			}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &checkedContext{Context: parent, checked: make(chan struct{})}
			cache.mu.Lock()
			done := make(chan error, 1)
			go func() {
				switch operation {
				case "get":
					_, _, err := cache.Get(ctx, "key")
					done <- err
				case "put":
					done <- cache.Put(ctx, "key", []byte("changed"), 0)
				case "delete":
					done <- cache.Delete(ctx, "key")
				}
			}()
			<-ctx.checked
			cancel()
			cache.mu.Unlock()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("operation ignored cancellation while queued: %v", err)
			}
			value, found, err := cache.Get(context.Background(), "key")
			if err != nil || !found || string(value) != "original" {
				t.Fatal("canceled operation changed cached data")
			}
		})
	}
}
