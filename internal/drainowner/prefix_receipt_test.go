package drainowner

import (
	"context"
	"errors"
	"testing"
)

func TestPrefixReceiptReconciliationRejectsAlteredEvidence(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "unexecuted", "foreign owner", "foreign frame", "wrong id", "foreign issuer", "wrong operation", "unselected", "unfinished", "duplicate snapshot"} {
		t.Run(mode, func(t *testing.T) {
			inv := NewInvocation()
			frame := inv.RootFrame()
			owner, handle := newJournalOwner(t, inv, frame)
			value := 1
			appendJournalRecord(t, owner, &value)
			executed := false
			owner.journalNative.Snapshot = func() []JournalRecord {
				r := JournalRecord{Record: &value, Frame: frame, ID: 1, Executed: executed}
				if mode == "duplicate snapshot" {
					return []JournalRecord{r, r}
				}
				return []JournalRecord{r}
			}
			owner.operations[PrefixPreparation] = func(ctx context.Context, permit *DrainPermit, _ error) error {
				record, err := ConsumeDrain(owner, permit, PrefixPreparation, nil)
				if err != nil {
					return err
				}
				defer EndDrain(record)
				rows := []JournalRecord{{Record: &value, Frame: frame, ID: 1}}
				if err = BindPrefixRecords(owner, permit, rows); err != nil {
					return err
				}
				executed = true
				rows[0].Executed = true
				return RecordPrefixStep(owner, permit, rows)
			}
			activity, err := AdmitFrameActivity(inv, frame)
			if err != nil {
				t.Fatal(err)
			}
			if err = EnrollActivities(inv); err != nil {
				t.Fatal(err)
			}
			if err = handle.CallPrefix(t.Context(), owner, inv, frame, activity, owner, "records", t.Context()); err != nil {
				t.Fatal(err)
			}
			receipt := inv.identity.journal.prefixReceipts[&value]
			switch mode {
			case "missing":
				delete(inv.identity.journal.prefixReceipts, &value)
			case "unexecuted":
				executed = false
			case "foreign owner":
				receipt.owner = NewState(new(int), nil, NativeOperations{})
			case "foreign frame":
				receipt.frame = NewInvocation().RootFrame()
			case "wrong id":
				receipt.id = 2
			case "foreign issuer":
				receipt.permit.identity = NewInvocation().identity
			case "wrong operation":
				receipt.permit.operation = Completion
			case "unselected":
				delete(receipt.permit.prefix.selected, &value)
			case "unfinished":
				receipt.permit.prefix.done = make(chan struct{})
			}
			if mode != "missing" {
				inv.identity.journal.prefixReceipts[&value] = receipt
			}
			if err = FinishActivity(inv, activity, nil); err != nil {
				t.Fatal(err)
			}
			if err = CloseActivities(inv); err != nil {
				t.Fatal(err)
			}
			ordered, err := inv.FreezeJournal()
			if mode == "valid" {
				if err != nil || !ordered || inv.NextJournalOwner() != nil {
					t.Fatalf("authenticated execution replayed: ordered=%v err=%v", ordered, err)
				}
			} else if !errors.Is(err, ErrJournal) {
				t.Fatalf("altered receipt accepted: %v", err)
			}
		})
	}
}

func TestNativeEffectBlocksEnrollmentBeforeAndAfterProtection(t *testing.T) {
	for _, enrolled := range []bool{false, true} {
		t.Run(map[bool]string{false: "dormant", true: "protected"}[enrolled], func(t *testing.T) {
			inv := NewInvocation()
			owner, _ := newJournalOwner(t, inv, inv.RootFrame())
			if enrolled {
				if err := EnrollActivities(inv); err != nil {
					t.Fatal(err)
				}
			}
			record, err := BeginNativeEffect(owner)
			if err != nil {
				t.Fatal(err)
			}
			if err = EnrollActivities(inv); !errors.Is(err, ErrDrainOverlap) {
				t.Fatalf("effect invisible to enrollment: %v", err)
			}
			EndDrain(record)
			if err = ProtectedFailure(inv); !errors.Is(err, ErrDrainOverlap) {
				t.Fatalf("caught overlap not terminal: %v", err)
			}
		})
	}
}

func TestPrefixAdmissionRejectsLaterOrdinaryDrainOwner(t *testing.T) {
	inv := NewInvocation()
	frame := inv.RootFrame()
	owner, handle := newJournalOwner(t, inv, frame)
	owner.operations[PrefixPreparation] = func(ctx context.Context, p *DrainPermit, _ error) error {
		record, err := ConsumeDrain(owner, p, PrefixPreparation, nil)
		if err != nil {
			return err
		}
		defer EndDrain(record)
		return BindPrefixRecords(owner, p, nil)
	}
	activity, err := AdmitFrameActivity(inv, frame)
	if err != nil {
		t.Fatal(err)
	}
	if err = EnrollActivities(inv); err != nil {
		t.Fatal(err)
	}
	if err = handle.CallPrefix(t.Context(), owner, inv, frame, activity, owner, "records", t.Context()); err != nil {
		t.Fatal(err)
	}
	bad := &journalOwner{frames: map[any]*Frame{}}
	bad.State = NewState(bad, func(p *Permit) error { return BindAttachment(bad, p) }, NativeOperations{})
	RegisterJournal(bad, NativeJournal{Bind: func(any, *Frame) error { return nil }, Snapshot: func() []JournalRecord { return nil }})
	// Retain genuine ordinary-drain evidence from before owner attachment.
	ordinary, err := BeginPublicDrain(bad)
	if err != nil {
		t.Fatal(err)
	}
	NoteJournalDrain(bad)
	EndDrain(ordinary)
	other, err := Claim(bad, inv)
	if err != nil {
		t.Fatal(err)
	}
	if err = other.Attach(bad, inv); err != nil {
		t.Fatal(err)
	}
	child, err := inv.ChildFrame(frame, "imperative", "")
	if err != nil {
		t.Fatal(err)
	}
	if err = other.BindFrame(bad, bad, inv, child); !errors.Is(err, ErrOrderedComposition) {
		t.Fatalf("later ordinary drain owner accepted: %v", err)
	}
	if err = ProtectedFailure(inv); !errors.Is(err, ErrOrderedComposition) {
		t.Fatalf("ownership denial not sticky: %v", err)
	}
}
