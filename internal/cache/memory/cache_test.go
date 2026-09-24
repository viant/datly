package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCapacityAndFIFO(t *testing.T) {
	if _, err := New(-1); err == nil {
		t.Fatal("negative capacity accepted")
	}
	cache, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, key := range []string{"a", "b"} {
		if err := cache.Put(ctx, key, []byte(key), 0); err != nil {
			t.Fatal(err)
		}
	}
	// Reading and updating do not move the original insertion position.
	if _, _, err := cache.Get(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(ctx, "a", []byte("updated"), 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(ctx, "c", []byte("c"), 0); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := cache.Get(ctx, "a"); found {
		t.Fatal("oldest insertion was not evicted")
	}
	for _, key := range []string{"b", "c"} {
		if _, found, err := cache.Get(ctx, key); err != nil || !found {
			t.Fatalf("%s found=%v err=%v", key, found, err)
		}
	}
	if err := cache.Delete(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if err := cache.Delete(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := cache.Get(ctx, "b"); found {
		t.Fatal("deleted entry found")
	}
}

func TestCopyTTLAndUnlimited(t *testing.T) {
	cache, err := New(0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	input := []byte("first")
	if err := cache.Put(ctx, "forever", input, 0); err != nil {
		t.Fatal(err)
	}
	input[0] = 'x'
	got, found, err := cache.Get(ctx, "forever")
	if err != nil || !found || string(got) != "first" {
		t.Fatalf("copy on Put: %q, %v, %v", got, found, err)
	}
	got[0] = 'x'
	got, found, err = cache.Get(ctx, "forever")
	if err != nil || !found || string(got) != "first" {
		t.Fatalf("copy on Get: %q, %v, %v", got, found, err)
	}
	if err := cache.Put(ctx, "nil", nil, 0); err != nil {
		t.Fatal(err)
	}
	if got, found, err := cache.Get(ctx, "nil"); err != nil || !found || got != nil {
		t.Fatalf("nil value: %v, %v, %v", got, found, err)
	}
	for i := 0; i < 50; i++ {
		if err := cache.Put(ctx, fmt.Sprint(i), []byte{byte(i)}, 0); err != nil {
			t.Fatal(err)
		}
	}
	if len(cache.entries) != 52 {
		t.Fatalf("unlimited cache retained %d entries", len(cache.entries))
	}
	if err := cache.Put(ctx, "short", []byte("value"), 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, found, err := cache.Get(ctx, "short"); err != nil || !found {
		t.Fatalf("fresh TTL entry: found=%v err=%v", found, err)
	}
	time.Sleep(120 * time.Millisecond)
	if _, found, err := cache.Get(ctx, "short"); err != nil || found {
		t.Fatalf("expired TTL entry: found=%v err=%v", found, err)
	}
	if err := cache.Put(ctx, "invalid", nil, -time.Second); err == nil {
		t.Fatal("negative TTL accepted")
	}
}

func TestExpiredEntryReclaimedBeforeLiveFIFOEntry(t *testing.T) {
	cache, _ := New(2)
	ctx := context.Background()
	if err := cache.Put(ctx, "live", []byte("keep"), 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(ctx, "expired", []byte("drop"), 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := cache.Put(ctx, "new", []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"live", "new"} {
		if _, found, err := cache.Get(ctx, key); err != nil || !found {
			t.Fatalf("%s found=%v err=%v", key, found, err)
		}
	}
}

func TestContextAndConcurrentAccess(t *testing.T) {
	cache, _ := New(8)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := cache.Get(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get err=%v", err)
	}
	if err := cache.Put(ctx, "x", nil, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put err=%v", err)
	}
	if err := cache.Delete(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete err=%v", err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprint((worker + i) % 12)
				if err := cache.Put(context.Background(), key, []byte(key), 0); err != nil {
					t.Error(err)
				}
				if _, _, err := cache.Get(context.Background(), key); err != nil {
					t.Error(err)
				}
				if i%3 == 0 {
					if err := cache.Delete(context.Background(), key); err != nil {
						t.Error(err)
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	if len(cache.entries) > 8 || cache.order.Len() != len(cache.entries) {
		t.Fatalf("capacity/order inconsistent: %d/%d", len(cache.entries), cache.order.Len())
	}
}
