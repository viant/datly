package drainowner

import (
	"errors"
	"sync"
	"testing"
)

func TestActivityLedgerLateEnrollmentTracksLiveAndFutureActivity(t *testing.T) {
	inv := NewInvocation()
	parent, e := AdmitActivity(inv)
	if e != nil {
		t.Fatal(e)
	}
	sibling, e := AdmitActivity(inv)
	if e != nil {
		t.Fatal(e)
	}
	if e = CheckActivities(inv); e != nil {
		t.Fatal("dormant activity changed ordinary policy", e)
	}
	if e = EnrollActivities(inv); e != nil {
		t.Fatal(e)
	}
	late, e := AdmitActivity(inv)
	if e != nil {
		t.Fatal(e)
	}
	if !errors.Is(CheckActivities(inv), ErrActivityUnfinished) {
		t.Fatal("late enrollment missed live activities")
	}
	if e = FinishActivity(inv, sibling, nil); e != nil {
		t.Fatal(e)
	}
	if e = FinishActivity(inv, late, nil); e != nil {
		t.Fatal(e)
	}
	if !errors.Is(CheckActivities(inv), ErrActivityUnfinished) {
		t.Fatal("child release retired composing parent")
	}
	if e = FinishActivity(inv, parent, nil); e != nil {
		t.Fatal(e)
	}
	if e = CloseActivities(inv); e != nil {
		t.Fatal(e)
	}
	if _, e = AdmitActivity(inv); !errors.Is(e, ErrActivityClosed) {
		t.Fatal(e)
	}
	if e = EnrollActivities(inv); !errors.Is(e, ErrActivityClosed) {
		t.Fatal(e)
	}
}
func TestActivityLedgerRetainsOnlyApplicableFailure(t *testing.T) {
	inv := NewInvocation()
	old, _ := AdmitActivity(inv)
	ordinary := errors.New("caught ordinary before enrollment")
	if e := FinishActivity(inv, old, ordinary); e != nil {
		t.Fatal(e)
	}
	live, _ := AdmitActivity(inv)
	if e := EnrollActivities(inv); e != nil {
		t.Fatal(e)
	}
	failed := errors.New("protected child failed")
	if e := FinishActivity(inv, live, failed); e != nil {
		t.Fatal(e)
	}
	got := CloseActivities(inv)
	if !errors.Is(got, failed) || errors.Is(got, ordinary) {
		t.Fatalf("wrong failure ownership: %v", got)
	}
	if e := FinishActivity(inv, live, nil); !errors.Is(e, ErrActivity) {
		t.Fatal(e)
	}
	if !errors.Is(CheckActivities(inv), failed) {
		t.Fatal("duplicate success clears failure")
	}
}
func TestActivityLedgerCopyForeignAndStaleCannotRetireOtherActivity(t *testing.T) {
	inv := NewInvocation()
	other := NewInvocation()
	token, _ := AdmitActivity(inv)
	copy := token
	ownOther, _ := AdmitActivity(other)
	if e := FinishActivity(other, copy, nil); !errors.Is(e, ErrActivity) {
		t.Fatal(e)
	}
	if e := FinishActivity(inv, ownOther, nil); !errors.Is(e, ErrActivity) {
		t.Fatal(e)
	}
	if e := EnrollActivities(inv); e != nil {
		t.Fatal(e)
	}
	if !errors.Is(CheckActivities(inv), ErrActivityUnfinished) {
		t.Fatal("foreign attempt removed live activity")
	}
	if e := FinishActivity(inv, copy, nil); e != nil {
		t.Fatal(e)
	}
	if e := FinishActivity(inv, token, nil); !errors.Is(e, ErrActivity) {
		t.Fatal(e)
	}
	if e := CloseActivities(inv); e != nil {
		t.Fatal(e)
	}
	if e := FinishActivity(other, ownOther, nil); e != nil {
		t.Fatal(e)
	}
}
func TestActivityLedgerClosureIsPermanentAndUnfinishedVetoSticky(t *testing.T) {
	inv := NewInvocation()
	token, _ := AdmitActivity(inv)
	if e := EnrollActivities(inv); e != nil {
		t.Fatal(e)
	}
	if e := CloseActivities(inv); !errors.Is(e, ErrActivityUnfinished) {
		t.Fatal(e)
	}
	if e := FinishActivity(inv, token, nil); e != nil {
		t.Fatal(e)
	}
	if !errors.Is(CheckActivities(inv), ErrActivityUnfinished) {
		t.Fatal("late retirement clears unfinished veto")
	}
	ordinary := NewInvocation()
	live, _ := AdmitActivity(ordinary)
	if e := CloseActivities(ordinary); e != nil {
		t.Fatal(e)
	}
	if _, e := AdmitActivity(ordinary); !errors.Is(e, ErrDormantActivityClosed) {
		t.Fatal(e)
	}
	if e := EnrollActivities(ordinary); !errors.Is(e, ErrActivityClosed) {
		t.Fatal(e)
	}
	if e := FinishActivity(ordinary, live, errors.New("dormant late error")); e != nil {
		t.Fatal(e)
	}
	if e := CheckActivities(ordinary); e != nil {
		t.Fatal(e)
	}
}
func TestActivityLedgerConcurrentCopiedRetirementExactlyOne(t *testing.T) {
	inv := NewInvocation()
	token, _ := AdmitActivity(inv)
	if e := EnrollActivities(inv); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	results := make(chan error, 64)
	for range 64 {
		wg.Add(1)
		go func() { defer wg.Done(); results <- FinishActivity(inv, token, nil) }()
	}
	wg.Wait()
	close(results)
	count := 0
	for e := range results {
		if e == nil {
			count++
		} else if !errors.Is(e, ErrActivity) {
			t.Fatal(e)
		}
	}
	if count != 1 {
		t.Fatalf("retired %d times", count)
	}
	if e := CloseActivities(inv); e != nil {
		t.Fatal(e)
	}
}
func TestActivityLedgerAdmissionEnrollmentClosureRace(t *testing.T) {
	for range 64 {
		inv := NewInvocation()
		start := make(chan struct{})
		results := make(chan error, 3)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			token, e := AdmitActivity(inv)
			if e == nil {
				e = FinishActivity(inv, token, nil)
			}
			results <- e
		}()
		go func() { defer wg.Done(); <-start; results <- EnrollActivities(inv) }()
		go func() { defer wg.Done(); <-start; results <- CloseActivities(inv) }()
		close(start)
		wg.Wait()
		close(results)
		for e := range results {
			if e != nil && !errors.Is(e, ErrDormantActivityClosed) && !errors.Is(e, ErrActivityClosed) && !errors.Is(e, ErrActivityUnfinished) {
				t.Fatal(e)
			}
		}
		if e := EnrollActivities(inv); !errors.Is(e, ErrActivityClosed) {
			t.Fatal("closed root re-enrolled", e)
		}
		if _, e := AdmitActivity(inv); !errors.Is(e, ErrActivityClosed) && !errors.Is(e, ErrDormantActivityClosed) {
			t.Fatal(e)
		}
	}
}
func TestActivityLedgerRejectsZeroIssuerAndToken(t *testing.T) {
	if _, e := AdmitActivity(nil); !errors.Is(e, ErrActivity) {
		t.Fatal(e)
	}
	if _, e := AdmitActivity(&Invocation{}); !errors.Is(e, ErrActivity) {
		t.Fatal(e)
	}
	inv := NewInvocation()
	if e := FinishActivity(inv, Activity{}, nil); !errors.Is(e, ErrActivity) {
		t.Fatal(e)
	}
	if e := EnrollActivities(nil); !errors.Is(e, ErrActivity) {
		t.Fatal(e)
	}
	if e := CloseActivities(nil); !errors.Is(e, ErrActivity) {
		t.Fatal(e)
	}
}
