package drainowner

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"sync"
	"testing"

	sqlxread "github.com/viant/sqlx/io/read"
)

func openTestBindingGroup(t *testing.T) (*Invocation, BindingGroup) {
	t.Helper()
	inv := NewInvocation()
	if err := EnrollActivities(inv); err != nil {
		t.Fatal(err)
	}
	group, err := OpenBindingGroup(inv, []BindingGroupMember{{Path: "a", Target: "GET:/a"}, {Path: "b", Target: "GET:/b"}, {Path: "path"}})
	if err != nil {
		t.Fatal(err)
	}
	return inv, group
}
func enterTestBindingRead(t *testing.T, inv *Invocation, g BindingGroup, path string) (context.Context, Activity) {
	t.Helper()
	ctx, err := g.Enter(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err = ValidateBindingGroupTarget(ctx, "GET:/"+path, true); err != nil {
		t.Fatal(err)
	}
	activity, err := AdmitBindingGroupActivity(ctx, inv)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, activity
}
func TestBindingGroupFailureImmediateIrreversibleAndCorrelated(t *testing.T) {
	inv, g := openTestBindingGroup(t)
	ctx, a := enterTestBindingRead(t, inv, g, "a")
	first := errors.New("actual first read failure")
	if err := FinishActivity(inv, a, first); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ProtectedFailure(inv), first) {
		t.Fatal("real failure delayed until group join")
	}
	observation, err := RecordBindingGroupFailure(g, "a", first, false)
	if err != nil || observation == 0 {
		t.Fatalf("correlation=%d %v", observation, err)
	}
	if err := FinishActivity(inv, a, nil); !errors.Is(err, ErrActivity) {
		t.Fatal("failed activity retired twice")
	}
	_, b := enterTestBindingRead(t, inv, g, "b")
	second := errors.New("actual second read failure")
	if err := FinishActivity(inv, b, second); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	ledger := ProtectedFailure(inv)
	if !errors.Is(ledger, first) || !errors.Is(ledger, second) {
		t.Fatal("root failure ledger cleared or filtered")
	}
	if _, err := AdmitBindingGroupActivity(ctx, inv); !errors.Is(err, ErrBindingGroup) {
		t.Fatal("retained ticket acquired authority")
	}
	if err := CloseActivities(inv); !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatal("completion lost failures")
	}
}
func TestBindingGroupIndependentEqualPointerAndTerminalCauses(t *testing.T) {
	cases := []struct {
		name        string
		cause       error
		independent bool
	}{{"equal independent pointer", errors.New("peer"), true}, {"cancel", context.Canceled, false}, {"deadline", context.DeadlineExceeded, false}, {"transaction done", sql.ErrTxDone, false}, {"bad connection", driver.ErrBadConn, false}, {"network", &net.OpError{Op: "read", Err: errors.New("closed")}, false}, {"returned cleanup", &sqlxread.CleanupError{Cause: errors.New("actual returned close")}, false}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv, g := openTestBindingGroup(t)
			_, a := enterTestBindingRead(t, inv, g, "a")
			if err := FinishActivity(inv, a, errors.Join(errors.New("wrapped stage"), tc.cause)); err != nil {
				t.Fatal(err)
			}
			if tc.independent {
				FailProtected(inv, tc.cause)
			}
			ctx, err := g.Enter(context.Background(), "b")
			if err == nil {
				t.Fatal("terminal failure allowed later member")
			}
			if ctx != nil {
				t.Fatal("failed enter returned a context")
			}
			if err := g.Close(); !errors.Is(err, tc.cause) {
				t.Fatalf("terminal close lost actual cause: %v", err)
			}
			if !errors.Is(ProtectedFailure(inv), tc.cause) {
				t.Fatal("root terminal ledger missing")
			}
		})
	}
}
func TestBindingGroupExactTicketsAndOrdinaryEffectAdmission(t *testing.T) {
	inv, g := openTestBindingGroup(t)
	ctx, a := enterTestBindingRead(t, inv, g, "a")
	foreign := NewInvocation()
	if _, err := AdmitBindingGroupActivity(ctx, foreign); !errors.Is(err, ErrBindingGroup) {
		t.Fatal("foreign invocation admitted")
	}
	if _, err := AdmitBindingGroupActivity(ctx, inv); !errors.Is(err, ErrBindingGroup) {
		t.Fatal("reused member admitted")
	}
	if err := ValidateBindingGroupTarget(ctx, "GET:/a", true); !errors.Is(err, ErrBindingGroup) {
		t.Fatal("target validation replayed")
	}

	if err := FinishActivity(inv, a, nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Enter(context.Background(), "b"); !errors.Is(err, ErrBindingGroup) {
		t.Fatal("unused ticket survived close")
	}
	if ProtectedFailure(inv) != nil {
		t.Fatal("successful group invented root failure")
	}
}

func TestBindingGroupUnrelatedAdmissionLatchesTerminal(t *testing.T) {
	inv, g := openTestBindingGroup(t)
	if _, err := AdmitActivity(inv); !errors.Is(err, ErrBindingGroup) {
		t.Fatal("unrelated activity admitted")
	}
	if !errors.Is(ProtectedFailure(inv), ErrBindingGroup) {
		t.Fatal("caught independent denial did not poison root")
	}
	if err := g.Close(); !errors.Is(err, ErrBindingGroup) {
		t.Fatal("group hid independent terminal denial")
	}
}

func TestBindingGroupPublicDrainAndOwnerOverlapRemainIndependentTerminal(t *testing.T) {
	for _, operation := range []string{"check public drain", "begin public drain", "owner attachment overlap"} {
		t.Run(operation, func(t *testing.T) {
			inv, g := openTestBindingGroup(t)
			owner := &attachmentTestOwner{}
			owner.State = NewState(owner, func(p *Permit) error { return BindAttachment(owner, p) })
			handle, err := Claim(owner, inv)
			if err != nil {
				t.Fatal(err)
			}
			expected := ErrDrain
			switch operation {
			case "check public drain":
				err = CheckPublicDrain(owner)
			case "begin public drain":
				_, err = BeginPublicDrain(owner)
			case "owner attachment overlap":
				// An existing native state-local drain is the audited owner-side overlap.
				owner.State.mu.Lock()
				record := addDrainLocked(owner.State, nil)
				owner.State.mu.Unlock()
				defer EndDrain(record)
				err = handle.Attach(owner, inv)
				expected = ErrDrainOverlap
			}
			if err == nil || !errors.Is(ProtectedFailure(inv), expected) {
				t.Fatalf("caught denial lost independent root cause %v", err)
			}
			if _, err = g.Enter(context.Background(), "path"); err == nil {
				t.Fatal("independent drain failure allowed group continuation")
			}
			if err = g.Close(); !errors.Is(err, expected) {
				t.Fatalf("close hid terminal drain cause %v", err)
			}
		})
	}
}

func TestBindingGroupTransactionStartExclusionAndOnceRelease(t *testing.T) {
	owner := &attachmentTestOwner{}
	owner.State = NewState(owner, nil)
	inv := NewInvocation()
	if _, err := Claim(owner, inv); err != nil {
		t.Fatal(err)
	}
	release, err := AdmitBindingGroupTransactionStart(owner)
	if err != nil {
		t.Fatal(err)
	}
	// A claimed but ordinary startup does not enroll or add any failure.
	if ActivitiesEnrolled(inv) || ProtectedFailure(inv) != nil {
		t.Fatal("default startup changed failure/enrollment")
	}
	var wait sync.WaitGroup
	for n := 0; n < 20; n++ {
		wait.Add(1)
		go func() { defer wait.Done(); release() }()
	}
	wait.Wait()
	if inv.identity.activities.transactionStarts != 0 {
		t.Fatal("release did not retire exactly once")
	}
	if err = EnrollActivities(inv); err != nil {
		t.Fatal(err)
	}
	group, err := OpenBindingGroup(inv, []BindingGroupMember{{Path: "path"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = AdmitBindingGroupTransactionStart(owner); !errors.Is(err, ErrBindingGroup) || !errors.Is(ProtectedFailure(inv), err) {
		t.Fatal("group winner did not retain exact independent start denial")
	}
	if err = group.Close(); !errors.Is(err, ErrBindingGroup) {
		t.Fatal("caught native startup denial disappeared")
	}
}
