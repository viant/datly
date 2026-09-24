package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingBackend struct {
	mu         sync.Mutex
	values     map[string][]byte
	getKey     string
	getStarted chan struct{}
	getRelease chan struct{}
	putKey     string
	putStarted chan struct{}
	putRelease chan struct{}
	getOnce    sync.Once
	putOnce    sync.Once
}

func (b *blockingBackend) Get(_ context.Context, key string) ([]byte, bool, error) {
	if key == b.getKey && b.getRelease != nil {
		b.getOnce.Do(func() { close(b.getStarted) })
		<-b.getRelease // Deliberately ignores context like a slow external backend.
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	value, ok := b.values[key]
	return append([]byte(nil), value...), ok, nil
}

func (b *blockingBackend) Put(_ context.Context, key string, value []byte, _ time.Duration) error {
	if key == b.putKey && b.putRelease != nil {
		b.putOnce.Do(func() { close(b.putStarted) })
		<-b.putRelease
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.values == nil {
		b.values = map[string][]byte{}
	}
	b.values[key] = append([]byte(nil), value...)
	return nil
}

func (b *blockingBackend) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	delete(b.values, key)
	b.mu.Unlock()
	return nil
}

func validUntil(expires time.Time) func([]byte) (time.Time, bool) {
	return func([]byte) (time.Time, bool) { return expires, true }
}

func outcome(value string, expires time.Time) func(context.Context) (Outcome, error) {
	return func(context.Context) (Outcome, error) { return Outcome{Value: []byte(value), Expires: expires}, nil }
}

func TestBlockedBackendGetDoesNotSerializeUnrelatedKey(t *testing.T) {
	backend := &blockingBackend{getKey: "0:0:slow", getStarted: make(chan struct{}), getRelease: make(chan struct{})}
	getRelease := backend.getRelease
	defer func() {
		select {
		case <-getRelease:
		default:
			close(getRelease)
		}
	}()
	coord := New(time.Now)
	expires := time.Now().Add(time.Minute)
	slow := make(chan error, 1)
	go func() {
		_, _, err := coord.Resolve(context.Background(), backend, "slow", "p", validUntil(expires), outcome("slow", expires))
		slow <- err
	}()
	<-backend.getStarted
	fast := make(chan error, 1)
	go func() {
		_, _, err := coord.Resolve(context.Background(), backend, "fast", "p", validUntil(expires), outcome("fast", expires))
		fast <- err
	}()
	select {
	case err := <-fast:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow backend Get serialized an unrelated key")
	}
	close(getRelease)
	if err := <-slow; err != nil {
		t.Fatal(err)
	}
}

func TestCanceledWaiterDoesNotWaitForLeader(t *testing.T) {
	backend := &blockingBackend{}
	coord := New(time.Now)
	expires := time.Now().Add(time.Minute)
	fetchStarted, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	leader := make(chan error, 1)
	go func() {
		_, _, err := coord.Resolve(context.Background(), backend, "same", "p", validUntil(expires), func(context.Context) (Outcome, error) {
			close(fetchStarted)
			<-release
			return Outcome{Value: []byte("value"), Expires: expires}, nil
		})
		leader <- err
	}()
	<-fetchStarted
	ctx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() {
		_, _, err := coord.Resolve(ctx, backend, "same", "p", validUntil(expires), outcome("wrong", expires))
		waiter <- err
	}()
	cancel()
	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled waiter blocked behind leader")
	}
	close(release)
	if err := <-leader; err != nil {
		t.Fatal(err)
	}
}

func TestInvalidateFencesSlowPutAndMetadataReaps(t *testing.T) {
	var tick atomic.Int64
	start := time.Now()
	clock := func() time.Time { return start.Add(time.Duration(tick.Load()) * time.Second) }
	backend := &blockingBackend{putKey: "0:0:key", putStarted: make(chan struct{}), putRelease: make(chan struct{})}
	coord := New(clock)
	expires := start.Add(time.Minute)
	leader := make(chan error, 1)
	go func() {
		_, _, err := coord.Resolve(context.Background(), backend, "key", "principal", validUntil(expires), outcome("old", expires))
		leader <- err
	}()
	<-backend.putStarted
	if _, err := coord.Invalidate(context.Background(), backend, "principal", false); err != nil {
		t.Fatal(err)
	}
	close(backend.putRelease)
	if err := <-leader; err != nil {
		t.Fatal(err)
	}
	if _, found, _ := backend.Get(context.Background(), "0:0:key"); found {
		t.Fatal("stale value repopulated after invalidation")
	}
	value, hit, err := coord.Resolve(context.Background(), backend, "key", "principal", validUntil(expires), outcome("new", expires))
	if err != nil || hit || string(value) != "new" {
		t.Fatalf("post-invalidation value=%q hit=%v err=%v", value, hit, err)
	}
	tick.Store(61)
	_, _, err = coord.Resolve(context.Background(), backend, "other", "principal", validUntil(clock().Add(time.Minute)), outcome("other", clock().Add(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	coord.mu.Lock()
	_, oldTracked := coord.partitions["principal"]["0:1:key"]
	coord.mu.Unlock()
	if oldTracked {
		t.Fatal("expired partition metadata survived a normal operation")
	}
}

func TestHighCardinalityExpiryReapingIsBoundedPerResolve(t *testing.T) {
	var tick atomic.Int64
	start := time.Now()
	clock := func() time.Time { return start.Add(time.Duration(tick.Load()) * time.Second) }
	backend := &blockingBackend{}
	coord := New(clock)
	for i := 0; i < 1500; i++ {
		key := fmt.Sprintf("principal-%d", i)
		expires := start.Add(time.Second)
		if _, _, err := coord.Resolve(context.Background(), backend, key, key, validUntil(expires), outcome(key, expires)); err != nil {
			t.Fatal(err)
		}
	}
	coord.mu.Lock()
	initial := len(coord.expires)
	coord.mu.Unlock()
	if initial != 1500 {
		t.Fatalf("tracked %d entries, want 1500", initial)
	}
	tick.Store(2)
	freshExpiry := clock().Add(time.Minute)
	resolveFresh := func() {
		t.Helper()
		if _, _, err := coord.Resolve(context.Background(), backend, "fresh", "fresh", validUntil(freshExpiry), outcome("fresh", freshExpiry)); err != nil {
			t.Fatal(err)
		}
	}
	resolveFresh()
	coord.mu.Lock()
	afterOne := len(coord.expires)
	coord.mu.Unlock()
	if afterOne != 1500-64+1 {
		t.Fatalf("one Resolve reaped %d expired entries, want bounded batch of 64", initial+1-afterOne)
	}
	for i := 0; i < 23; i++ {
		resolveFresh()
	}
	coord.mu.Lock()
	remaining := len(coord.expires)
	coord.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("expired metadata retained after amortized reaping: %d entries", remaining)
	}
}
