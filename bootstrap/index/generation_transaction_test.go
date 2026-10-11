package index

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	dexec "github.com/viant/datly/exec"
)

func TestGenerationTransactionLoadDrainsCancellationAndRetries(t *testing.T) {
	snapshot := testSnapshot(t, "Records")
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	materializer := MaterializeFunc(func(ctx context.Context, entry *Entry, _ Resolver) (*Loaded, error) {
		if !dexec.HasInvocationTransactionLookup(ctx) {
			t.Error("invocation lookup lost during materialization")
		}
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			close(cancelled)
			<-release // simulates statement/row cleanup after cancellation
			return nil, ctx.Err()
		}
		return loadedEntry(entry, nil), nil
	})
	g := newGeneration(1, snapshot, materializer)
	defer g.retire()
	lookup := func(context.Context, *sql.DB) (*sql.Tx, error) { return nil, nil }
	ctx, cancel := context.WithCancel(dexec.WithInvocationTransactionLookup(context.Background(), lookup))
	returned := make(chan error, 1)
	go func() { _, err := g.Load(ctx, snapshot.entries[0].Key()); returned <- err }()
	<-started
	waiter, cancelWaiter := context.WithCancel(context.Background())
	cancelWaiter()
	if _, err := g.Load(waiter, snapshot.entries[0].Key()); !errors.Is(err, context.Canceled) {
		t.Fatalf("secondary waiter cancellation: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("waiter restarted owner materialization")
	}
	cancel()
	<-cancelled
	select {
	case err := <-returned:
		t.Fatalf("owner returned before cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	healthy := dexec.WithInvocationTransactionLookup(context.Background(), lookup)
	if _, err := g.Load(healthy, snapshot.entries[0].Key()); err != nil {
		t.Fatalf("retry poisoned by cancellation: %v", err)
	}
	if _, err := g.Load(context.Background(), snapshot.entries[0].Key()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("successful load not cached: %d", calls.Load())
	}
}

func TestGenerationTransactionLookupReachesNestedDependency(t *testing.T) {
	snapshot := testSnapshot(t, "Parent", "Child")
	lookup := func(context.Context, *sql.DB) (*sql.Tx, error) { return nil, nil }
	var calls atomic.Int32
	g := newGeneration(1, snapshot, MaterializeFunc(func(ctx context.Context, entry *Entry, resolver Resolver) (*Loaded, error) {
		if !dexec.HasInvocationTransactionLookup(ctx) {
			t.Error("nested load lost transaction lookup")
		}
		calls.Add(1)
		if entry.Component.Name == "Parent" {
			for _, child := range snapshot.entries {
				if child.Component.Name == "Child" {
					if _, err := resolver.Load(ctx, child.Key()); err != nil {
						return nil, err
					}
				}
			}
		}
		return loadedEntry(entry, nil), nil
	}))
	defer g.retire()
	for _, entry := range snapshot.entries {
		if entry.Component.Name == "Parent" {
			if _, err := g.Load(dexec.WithInvocationTransactionLookup(context.Background(), lookup), entry.Key()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("nested loads = %d", calls.Load())
	}
}
