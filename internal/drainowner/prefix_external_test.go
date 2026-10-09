package drainowner

import (
	"context"
	"errors"
	"testing"
)

func externalPrefixFixture(t *testing.T) (*Invocation, *journalOwner, Handle, Activity) {
	t.Helper()
	inv := NewInvocation()
	frame := inv.RootFrame()
	owner, handle := newJournalOwner(t, inv, frame, true)
	owner.operations[PrefixPreparation] = func(ctx context.Context, p *DrainPermit, _ error) error {
		r, err := ConsumeDrain(owner, p, PrefixPreparation, nil)
		if err != nil {
			return err
		}
		defer EndDrain(r)
		if !ExternalPrefix(owner, p) {
			t.Fatal("external mode not authenticated")
		}
		return BindPrefixRecords(owner, p, nil)
	}
	activity, err := AdmitFrameActivity(inv, frame)
	if err != nil {
		t.Fatal(err)
	}
	if err = EnrollActivities(inv); err != nil {
		t.Fatal(err)
	}
	return inv, owner, handle, activity
}

func TestExternalPrefixRejectsCompositionBeforeBoundary(t *testing.T) {
	for _, mode := range []string{"buffered", "mixed owner", "open child", "closed child writes", "child caller", "prior drain", "concurrent activity"} {
		t.Run(mode, func(t *testing.T) {
			inv, owner, handle, activity := externalPrefixFixture(t)
			frame := inv.RootFrame()
			switch mode {
			case "buffered":
				if err := inv.EnableJournal(); err != nil {
					t.Fatal(err)
				}
			case "mixed owner":
				newJournalOwner(t, inv, frame)
			case "open child":
				if _, err := inv.ChildFrame(frame, "imperative", ""); err != nil {
					t.Fatal(err)
				}
			case "closed child writes":
				child, err := inv.ChildFrame(frame, "imperative", "")
				if err != nil {
					t.Fatal(err)
				}
				value := 1
				if err = AppendJournal(owner, child, &value); err != nil {
					t.Fatal(err)
				}
				SealFrame(child)
			case "child caller":
				child, err := inv.ChildFrame(frame, "imperative", "")
				if err != nil {
					t.Fatal(err)
				}
				if err = FinishActivity(inv, activity, nil); err != nil {
					t.Fatal(err)
				}
				activity, err = AdmitFrameActivity(inv, child)
				if err != nil {
					t.Fatal(err)
				}
				frame = child
			case "prior drain":
				NoteJournalDrain(owner)
			case "concurrent activity":
				if _, err := AdmitFrameActivity(inv, frame); err != nil {
					t.Fatal(err)
				}
			}
			if err := handle.CallPrefix(t.Context(), owner, inv, frame, activity, owner, "records", t.Context()); !errors.Is(err, ErrPrefix) {
				t.Fatalf("accepted %s: %v", mode, err)
			}
			if !errors.Is(ProtectedFailure(inv), ErrPrefix) {
				t.Fatal("denial not sticky")
			}
		})
	}
}

func TestExternalPrefixNoMatchRetainsIsolation(t *testing.T) {
	for _, mode := range []string{"child", "activity", "binding group", "buffered", "new owner", "public drain"} {
		t.Run(mode, func(t *testing.T) {
			inv, owner, handle, activity := externalPrefixFixture(t)
			frame := inv.RootFrame()
			for n := 0; n < 2; n++ {
				if err := handle.CallPrefix(t.Context(), owner, inv, frame, activity, owner, "unused", t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			value := 1
			appendJournalRecord(t, owner, &value) // root suffix remains legal
			var err error
			switch mode {
			case "child":
				_, err = inv.ChildFrame(frame, "imperative", "")
			case "activity":
				_, err = AdmitFrameActivity(inv, frame)
			case "binding group":
				_, err = OpenBindingGroup(inv, []BindingGroupMember{{Path: "child"}})
			case "buffered":
				err = inv.EnableJournal()
			case "new owner":
				other := &journalOwner{frames: map[any]*Frame{}}
				other.State = NewState(other, func(p *Permit) error { return BindAttachment(other, p) }, NativeOperations{})
				RegisterJournal(other, NativeJournal{Bind: func(any, *Frame) error { return nil }, Snapshot: func() []JournalRecord { return nil }})
				h, e := Claim(other, inv)
				if e != nil {
					t.Fatal(e)
				}
				if e = h.Attach(other, inv); e != nil {
					t.Fatal(e)
				}
				err = h.BindFrame(other, other, inv, frame)
			case "public drain":
				err = CheckPublicDrain(owner)
			}
			if err == nil || ProtectedFailure(inv) == nil {
				t.Fatalf("late %s not terminal: %v", mode, err)
			}
		})
	}
}
