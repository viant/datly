package drainowner

import (
	"errors"
	"sync"
	"testing"
)

func TestActivityRetainsExactOpenInvocationFrame(t *testing.T) {
	invocation := NewInvocation()
	root := invocation.RootFrame()
	child, err := invocation.ChildFrame(root, "buffered", "")
	if err != nil {
		t.Fatal(err)
	}
	activity, err := AdmitFrameActivity(invocation, child)
	if err != nil || activity.cell.frame != child {
		t.Fatalf("lost exact frame: activity=%+v err=%v", activity, err)
	}
	foreign := NewInvocation().RootFrame()
	for _, frame := range []*Frame{nil, foreign} {
		if _, err := AdmitFrameActivity(invocation, frame); !errors.Is(err, ErrActivity) {
			t.Fatalf("admitted foreign or absent frame: %v", err)
		}
	}
	SealFrame(child)
	if _, err := AdmitFrameActivity(invocation, child); !errors.Is(err, ErrActivity) {
		t.Fatalf("admitted sealed frame: %v", err)
	}
	// Sealing a component cannot silently retire its in-flight activity.
	if err := EnrollActivities(invocation); err != nil {
		t.Fatal(err)
	}
	if err := CheckActivities(invocation); !errors.Is(err, ErrActivityUnfinished) {
		t.Fatal(err)
	}
	if err := FinishActivity(invocation, activity, nil); err != nil {
		t.Fatal(err)
	}
	if err := CloseActivities(invocation); err != nil {
		t.Fatal(err)
	}
	if _, err := AdmitFrameActivity(invocation, root); !errors.Is(err, ErrActivityClosed) {
		t.Fatalf("closed admission changed: %v", err)
	}
}

func TestFrameActivityDormantClosureAndRetainedSibling(t *testing.T) {
	invocation := NewInvocation()
	root := invocation.RootFrame()
	child, _ := invocation.ChildFrame(root, "imperative", "")
	prior, err := AdmitFrameActivity(invocation, child)
	if err != nil {
		t.Fatal(err)
	}
	SealFrame(child)
	if err := FinishActivity(invocation, prior, nil); err != nil {
		t.Fatal(err)
	}
	sibling, err := invocation.ChildFrame(child, "imperative", "")
	if err != nil || sibling.parent != root {
		t.Fatalf("retained context failed to use open ancestor: %v", err)
	}
	current, err := AdmitFrameActivity(invocation, sibling)
	if err != nil || current.cell == prior.cell || current.cell.frame != sibling {
		t.Fatalf("retained activity was reused: %v", err)
	}
	if err := FinishActivity(invocation, current, nil); err != nil {
		t.Fatal(err)
	}
	if err := CloseActivities(invocation); err != nil {
		t.Fatal(err)
	}
	// Closure classification takes precedence even for an already sealed frame.
	if _, err := AdmitFrameActivity(invocation, child); !errors.Is(err, ErrDormantActivityClosed) {
		t.Fatal(err)
	}
}

func TestFrameActivitySealAdmissionRace(t *testing.T) {
	for range 100 {
		invocation := NewInvocation()
		frame, _ := invocation.ChildFrame(invocation.RootFrame(), "imperative", "")
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var activity Activity
		var err error
		go func() { defer wg.Done(); <-start; activity, err = AdmitFrameActivity(invocation, frame) }()
		go func() { defer wg.Done(); <-start; SealFrame(frame) }()
		close(start)
		wg.Wait()
		if err == nil {
			if activity.cell.frame != frame {
				t.Fatal("admission changed identity")
			}
			if err := FinishActivity(invocation, activity, nil); err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, ErrActivity) {
			t.Fatal(err)
		}
		if err := CloseActivities(invocation); err != nil {
			t.Fatalf("seal or retirement leaked an activity: %v", err)
		}
	}
}
