package index

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func testSnapshot(t testing.TB, names ...string) *Snapshot {
	t.Helper()
	entries := make([]*Entry, 0, len(names))
	for _, name := range names {
		component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: name}, Name: name, Routes: []*spec.Route{{Method: "GET", Path: "/" + name}}}
		entries = append(entries, &Entry{Component: component, Fingerprint: name})
	}
	sortEntries(entries)
	snapshot, err := newSnapshot("test", digestStrings(names...), entries)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func loadedEntry(entry *Entry, release func(context.Context) error) *Loaded {
	return &Loaded{Registration: &registry.RegisteredComponent{Component: entry.Component.Clone()}, Release: release}
}

func TestLeaseLookupAndGateDoNotMaterialize(t *testing.T) {
	snapshot := testSnapshot(t, "Users")
	var calls atomic.Int32
	registry := &Registry{}
	if _, err := registry.Publish(snapshot, MaterializeFunc(func(_ context.Context, entry *Entry, _ Resolver) (*Loaded, error) {
		calls.Add(1)
		return loadedEntry(entry, nil), nil
	})); err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Pin()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if _, _, _, ok := lease.Snapshot().Route("GET", "/Users"); !ok || calls.Load() != 0 {
		t.Fatalf("route lookup loaded component: found=%v calls=%d", ok, calls.Load())
	}
	denied := errors.New("denied")
	if _, _, err := lease.LoadRoute(context.Background(), "GET", "/Users", func(context.Context, *Entry, *spec.Route) error { return denied }); !errors.Is(err, denied) {
		t.Fatalf("gate error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("authorization gate ran after materialization")
	}
}

func TestGenerationSingleflightCachesSuccessAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure_%v", fail), func(t *testing.T) {
			snapshot := testSnapshot(t, "Users")
			var calls atomic.Int32
			gate := make(chan struct{})
			started := make(chan struct{})
			materializer := MaterializeFunc(func(_ context.Context, entry *Entry, _ Resolver) (*Loaded, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-gate
				if fail {
					return nil, errors.New("deterministic compile failure")
				}
				return loadedEntry(entry, nil), nil
			})
			registry := &Registry{}
			if _, err := registry.Publish(snapshot, materializer); err != nil {
				t.Fatal(err)
			}
			lease, _ := registry.Pin()
			defer lease.Close()
			key := snapshot.entries[0].Key()
			const parallel = 32
			errs := make(chan error, parallel)
			var wait sync.WaitGroup
			wait.Add(parallel)
			for index := 0; index < parallel; index++ {
				go func() {
					defer wait.Done()
					_, err := lease.LoadComponent(context.Background(), key)
					errs <- err
				}()
			}
			<-started
			close(gate)
			wait.Wait()
			close(errs)
			for err := range errs {
				if fail != (err != nil) {
					t.Fatalf("load error = %v", err)
				}
			}
			if _, err := lease.LoadComponent(context.Background(), key); fail != (err != nil) {
				t.Fatalf("cached load error = %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("materializations = %d", calls.Load())
			}
		})
	}
}

func TestMaterializerDependenciesUseSameGeneration(t *testing.T) {
	snapshot := testSnapshot(t, "Parent", "Child")
	var parentCalls, childCalls atomic.Int32
	materializer := MaterializeFunc(func(ctx context.Context, entry *Entry, resolver Resolver) (*Loaded, error) {
		if entry.Key().Name == "Parent" {
			parentCalls.Add(1)
			if _, err := resolver.Load(ctx, spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Child"}); err != nil {
				return nil, err
			}
		} else {
			childCalls.Add(1)
		}
		return loadedEntry(entry, nil), nil
	})
	registry := &Registry{}
	if _, err := registry.Publish(snapshot, materializer); err != nil {
		t.Fatal(err)
	}
	lease, _ := registry.Pin()
	defer lease.Close()
	if _, err := lease.LoadComponent(context.Background(), spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Parent"}); err != nil {
		t.Fatal(err)
	}
	if parentCalls.Load() != 1 || childCalls.Load() != 1 {
		t.Fatalf("dependency calls parent=%d child=%d", parentCalls.Load(), childCalls.Load())
	}
}

func TestReloadKeepsInflightLoadGenerationScopedAndReleasesAfterLease(t *testing.T) {
	registry := &Registry{}
	firstSnapshot := testSnapshot(t, "Users")
	started := make(chan struct{})
	continueLoad := make(chan struct{})
	released := make(chan struct{}, 2)
	first, err := registry.Publish(firstSnapshot, MaterializeFunc(func(_ context.Context, entry *Entry, _ Resolver) (*Loaded, error) {
		close(started)
		<-continueLoad
		return loadedEntry(entry, func(context.Context) error { released <- struct{}{}; return nil }), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	oldLease, _ := registry.Pin()
	oldResult := make(chan error, 1)
	go func() {
		_, oldResultErr := oldLease.LoadComponent(context.Background(), firstSnapshot.entries[0].Key())
		oldResult <- oldResultErr
	}()
	<-started
	secondSnapshot := testSnapshot(t, "Users")
	second, err := registry.Publish(secondSnapshot, MaterializeFunc(func(_ context.Context, entry *Entry, _ Resolver) (*Loaded, error) {
		return loadedEntry(entry, func(context.Context) error { released <- struct{}{}; return nil }), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatal("reload reused generation identity")
	}
	newLease, _ := registry.Pin()
	if newLease.GenerationID() != second.ID {
		t.Fatal("new pin did not observe atomically published generation")
	}
	if _, err := newLease.LoadComponent(context.Background(), secondSnapshot.entries[0].Key()); err != nil {
		t.Fatal(err)
	}
	close(continueLoad)
	if err := <-oldResult; err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
		t.Fatal("retired generation released while its request lease was pinned")
	default:
	}
	oldLease.Close()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("retired generation did not release its loaded resources")
	}
	newLease.Close()
	if err := registry.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
	default:
		t.Fatal("active generation did not release its loaded resources on shutdown")
	}
}

func TestMaterializationCycleFailsWithoutDeadlock(t *testing.T) {
	snapshot := testSnapshot(t, "A", "B")
	materializer := MaterializeFunc(func(ctx context.Context, entry *Entry, resolver Resolver) (*Loaded, error) {
		dependency := "B"
		if entry.Key().Name == "B" {
			dependency = "A"
		}
		if _, err := resolver.Load(ctx, spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: dependency}); err != nil {
			return nil, err
		}
		return loadedEntry(entry, nil), nil
	})
	registry := &Registry{}
	if _, err := registry.Publish(snapshot, materializer); err != nil {
		t.Fatal(err)
	}
	lease, _ := registry.Pin()
	defer lease.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := lease.LoadComponent(ctx, spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "A"})
	if err == nil || ctx.Err() != nil {
		t.Fatalf("cycle error = %v, context = %v", err, ctx.Err())
	}
}
