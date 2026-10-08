package reader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRelationScheduler_BoundsWorkAndJoins(t *testing.T) {
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

func TestRelationScheduler_ReleasedCapacityStartsPendingSibling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scheduler := newRelationScheduler(ctx, 4)
	defer scheduler.Close()
	reserved := make(chan struct{}, 3)
	releaseSlot := make(chan struct{})
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	lookupStarted := make(chan struct{})
	work := make([]relationWork, 0, 5)
	for i := 0; i < 3; i++ {
		work = append(work, relationWork{run: func(ctx context.Context) error {
			reserved <- struct{}{}
			select {
			case <-releaseSlot:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}})
	}
	work = append(work, relationWork{run: func(ctx context.Context) error {
		close(slowStarted)
		select {
		case <-releaseSlow:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}, relationWork{run: func(context.Context) error { close(lookupStarted); return nil }})
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(work) }()
	for i := 0; i < 3; i++ {
		awaitRelationSignal(t, ctx, reserved)
	}
	awaitRelationSignal(t, ctx, slowStarted)
	select {
	case <-lookupStarted:
		t.Fatal("lookup started while all four workers were occupied")
	default:
	}
	select {
	case releaseSlot <- struct{}{}:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	awaitRelationSignal(t, ctx, lookupStarted)
	// The slow task is still blocked: it cannot be responsible for this progress.
	close(releaseSlow)
	close(releaseSlot)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRelationScheduler_RecursiveCompletionAndBound(t *testing.T) {
	for _, concurrency := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			scheduler := newRelationScheduler(ctx, concurrency)
			defer scheduler.Close()
			var active, maximum, completed, finished atomic.Int32
			var node func(int) relationWork
			node = func(depth int) relationWork {
				var descendants, descendantsDone atomic.Int32
				phase := func(context.Context) error {
					current := active.Add(1)
					defer active.Add(-1)
					for observed := maximum.Load(); current > observed && !maximum.CompareAndSwap(observed, current); observed = maximum.Load() {
					}
					return nil
				}
				return relationWork{
					run: phase,
					children: func() []relationWork {
						if depth == 0 {
							return nil
						}
						descendants.Store(3)
						children := make([]relationWork, 3)
						for i := range children {
							child := node(depth - 1)
							finish := child.finish
							child.finish = func(err error) { finish(err); descendantsDone.Add(1) }
							children[i] = child
						}
						return children
					},
					complete: func(ctx context.Context) error {
						if descendants.Load() != descendantsDone.Load() {
							return errors.New("parent completed before its descendants")
						}
						completed.Add(1)
						return phase(ctx)
					},
					finish: func(error) { finished.Add(1) },
				}
			}
			if err := scheduler.Run([]relationWork{node(4), node(4)}); err != nil {
				t.Fatal(err)
			}
			if maximum.Load() > int32(concurrency) || active.Load() != 0 || completed.Load() != 242 || finished.Load() != 242 {
				t.Fatalf("bound/lifecycle: max=%d active=%d complete=%d finish=%d", maximum.Load(), active.Load(), completed.Load(), finished.Load())
			}
		})
	}
}

func TestRelationScheduler_ParentCompletionDoesNotWaitForUnrelatedBranch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scheduler := newRelationScheduler(ctx, 2)
	defer scheduler.Close()
	blocked := make(chan struct{})
	release := make(chan struct{})
	parentDone := make(chan struct{})
	work := []relationWork{{
		children: func() []relationWork { return []relationWork{{}} },
		complete: func(context.Context) error { close(parentDone); return nil },
	}, {run: func(ctx context.Context) error {
		close(blocked)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}}
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(work) }()
	awaitRelationSignal(t, ctx, blocked)
	awaitRelationSignal(t, ctx, parentDone)
	select {
	case <-done:
		t.Fatal("Run returned before joining the unrelated branch")
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRelationScheduler_RecursiveFailureCleanup(t *testing.T) {
	for _, phase := range []string{"run", "children", "complete", "finish", "cancel"} {
		for _, panics := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/panic=%v", phase, panics), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				scheduler := newRelationScheduler(ctx, 1)
				defer scheduler.Close()
				var mu sync.Mutex
				started, finished, skipped := map[string]int{}, map[string]int{}, map[string]int{}
				boom := errors.New("failure")
				fail := func() error {
					if phase == "cancel" {
						cancel()
						return ctx.Err()
					}
					if panics {
						panic("private failure")
					}
					return boom
				}
				makeWork := func(name string) relationWork {
					return relationWork{
						run:    func(context.Context) error { mu.Lock(); started[name]++; mu.Unlock(); return nil },
						finish: func(error) { mu.Lock(); finished[name]++; mu.Unlock() },
						skip:   func() { mu.Lock(); skipped[name]++; mu.Unlock() },
					}
				}
				parent, child, sibling := makeWork("parent"), makeWork("child"), makeWork("sibling")
				if phase == "run" || phase == "cancel" {
					childRun := child.run
					child.run = func(ctx context.Context) error { _ = childRun(ctx); return fail() }
				} else if phase == "children" {
					// Child discovery has no error return; failure here is a panic.
					child.children = func() []relationWork { panic("child discovery failed") }
				} else if phase == "complete" {
					child.complete = func(context.Context) error { return fail() }
				} else {
					finish := child.finish
					child.finish = func(err error) { finish(err); panic("cleanup failed") }
				}
				parent.children = func() []relationWork { return []relationWork{child, sibling} }
				var parentCompleted bool
				parent.complete = func(context.Context) error { parentCompleted = true; return nil }
				if err := scheduler.Run([]relationWork{parent}); err == nil {
					t.Fatal("expected failure")
				}
				if parentCompleted || started["parent"] != 1 || finished["parent"] != 1 || started["child"] != 1 || finished["child"] != 1 || skipped["sibling"] != 1 || started["sibling"] != 0 || finished["sibling"] != 0 || skipped["parent"] != 0 || skipped["child"] != 0 {
					t.Fatalf("cleanup: started=%v finished=%v skipped=%v parentCompleted=%v", started, finished, skipped, parentCompleted)
				}
			})
		}
	}
}

func awaitRelationSignal(t *testing.T, ctx context.Context, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestRelationScheduler_NestedFailureJoinsBeforeParentCleanup(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%v", panics), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			scheduler := newRelationScheduler(ctx, 2)
			defer scheduler.Close()
			entered := make(chan struct{})
			var active, cleaned, skipped atomic.Int32
			var parentCleaned, parentCompleted, cleanedEarly bool
			parent := relationWork{
				children: func() []relationWork {
					return []relationWork{{
						run: func(ctx context.Context) error {
							select {
							case <-entered:
							case <-ctx.Done():
								return ctx.Err()
							}
							if panics {
								panic("nested failure")
							}
							return errors.New("nested failure")
						},
						finish: func(error) { cleaned.Add(1) },
					}, {
						run: func(ctx context.Context) error {
							active.Add(1)
							defer active.Add(-1)
							close(entered)
							<-ctx.Done()
							return ctx.Err()
						},
						finish: func(error) { cleaned.Add(1) },
					}, {skip: func() { skipped.Add(1) }}}
				},
				complete: func(context.Context) error { parentCompleted = true; return nil },
				finish: func(error) {
					parentCleaned = true
					cleanedEarly = active.Load() != 0 || cleaned.Load() != 2 || skipped.Load() != 1
				},
			}
			if err := scheduler.Run([]relationWork{parent}); err == nil {
				t.Fatal("expected nested failure")
			}
			if !parentCleaned || parentCompleted || cleanedEarly {
				t.Fatalf("parent cleanup: cleaned=%v completed=%v early=%v", parentCleaned, parentCompleted, cleanedEarly)
			}
		})
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
