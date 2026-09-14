package reader

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRelationScheduler_BoundsRecursiveWorkAndJoins(t *testing.T) {
	scheduler := newRelationScheduler(context.Background(), 2)
	defer scheduler.Close()
	entered := make(chan struct{}, 5)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	var calls atomic.Int32
	work := make([]relationWork, 5)
	for i := range work {
		work[i].run = func(context.Context) error {
			current := active.Add(1)
			for observed := maximum.Load(); current > observed && !maximum.CompareAndSwap(observed, current); observed = maximum.Load() {
			}
			calls.Add(1)
			entered <- struct{}{}
			<-release
			active.Add(-1)
			return nil
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- scheduler.Run(work)
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("scheduler did not start two bounded workers")
		}
	}
	select {
	case <-entered:
		t.Fatal("scheduler exceeded the configured concurrency")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("scheduler failed: %v", err)
	}
	if calls.Load() != 5 || maximum.Load() != 2 || active.Load() != 0 {
		t.Fatalf("unexpected scheduler state calls=%d max=%d active=%d", calls.Load(), maximum.Load(), active.Load())
	}
}

func TestRelationScheduler_CancelsJoinsAndReleasesSkippedWork(t *testing.T) {
	scheduler := newRelationScheduler(context.Background(), 2)
	defer scheduler.Close()
	boom := errors.New("relation failed")
	inlineStarted := make(chan struct{})
	var active atomic.Int32
	var skipped atomic.Int32
	var startedAfterCancel atomic.Int32
	work := make([]relationWork, 5)
	work[0].run = func(context.Context) error {
		active.Add(1)
		defer active.Add(-1)
		<-inlineStarted
		return boom
	}
	work[1].run = func(ctx context.Context) error {
		active.Add(1)
		defer active.Add(-1)
		close(inlineStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	for i := 2; i < len(work); i++ {
		work[i] = relationWork{
			run: func(context.Context) error {
				startedAfterCancel.Add(1)
				return nil
			},
			skip: func() { skipped.Add(1) },
		}
	}
	err := scheduler.Run(work)
	if !errors.Is(err, boom) {
		t.Fatalf("scheduler error: got %v, want %v", err, boom)
	}
	if skipped.Load() != 3 || active.Load() != 0 || startedAfterCancel.Load() != 0 {
		t.Fatalf("unexpected cancellation state skipped=%d active=%d startedAfterCancel=%d", skipped.Load(), active.Load(), startedAfterCancel.Load())
	}
}
