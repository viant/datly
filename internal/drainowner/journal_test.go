package drainowner

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

type journalOwner struct {
	*State
	frame    *Frame
	records  []any
	frames   map[any]*Frame
	executed []any
	permits  []*DrainPermit
	fail     bool
}

func newJournalOwner(t *testing.T, i *Invocation, frame *Frame) (*journalOwner, Handle) {
	t.Helper()
	o := &journalOwner{frames: map[any]*Frame{}}
	prepare := func(ctx context.Context, p *DrainPermit, _ error) error {
		o.permits = append(o.permits, p)
		// Copies, foreign operation identities, and forged tokens are rejected even
		// during a genuine constructor callback, before the legitimate consumption.
		copy := *p
		if _, err := ConsumeDrain(o, &copy, p.operation, nil); !errors.Is(err, ErrDrainPermit) {
			t.Fatalf("copied=%v", err)
		}
		if _, err := ConsumeDrain(o, &DrainPermit{}, p.operation, nil); !errors.Is(err, ErrDrainPermit) {
			t.Fatalf("forged=%v", err)
		}
		record, err := ConsumeDrain(o, p, p.operation, nil)
		if err != nil {
			return err
		}
		defer EndDrain(record)
		values, err := RunRecords(o, p)
		if err != nil {
			return err
		}
		if _, err := RunRecords(o, &copy); !errors.Is(err, ErrDrainPermit) {
			t.Fatalf("copied range=%v", err)
		}
		if o.fail {
			return errors.New("native execution failure")
		}
		o.executed = append(o.executed, values...)
		return nil
	}
	o.State = NewState(o, func(p *Permit) error { return BindAttachment(o, p) }, NativeOperations{LocalPreparation: prepare, AllPreparation: prepare, Completion: prepare, Abort: func(_ context.Context, p *DrainPermit, cause error) error {
		r, e := ConsumeDrain(o, p, Abort, cause)
		if e != nil {
			return e
		}
		EndDrain(r)
		return cause
	}})
	RegisterJournal(o, NativeJournal{Bind: func(component any, f *Frame) error {
		if component != o {
			return ErrJournal
		}
		o.frame = f
		return nil
	}, Snapshot: func() []JournalRecord {
		result := make([]JournalRecord, len(o.records))
		for n, r := range o.records {
			result[n] = JournalRecord{Record: r, Frame: o.frames[r], ID: uint64(n + 1)}
		}
		return result
	}})
	h, err := Claim(o, i)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Attach(o, i); err != nil {
		t.Fatal(err)
	}
	if err = h.BindFrame(o, o, i, frame); err != nil {
		t.Fatal(err)
	}
	return o, h
}
func appendJournalRecord(t *testing.T, o *journalOwner, value *int) {
	t.Helper()
	if err := AppendJournal(o, o.frame, value); err != nil {
		t.Fatal(err)
	}
	o.frames[value] = o.frame
	o.records = append(o.records, value)
}
func journalFixture(t *testing.T) (*Invocation, *journalOwner, Handle, *journalOwner, Handle, []any) {
	t.Helper()
	i := NewInvocation()
	root := i.RootFrame()
	a, ha := newJournalOwner(t, i, root)
	one, two, three := 1, 2, 3
	appendJournalRecord(t, a, &one)
	child, err := i.ChildFrame(root, "buffered_imperative", "")
	if err != nil {
		t.Fatal(err)
	}
	b, hb := newJournalOwner(t, i, child)
	appendJournalRecord(t, b, &two)
	SealFrame(child)
	appendJournalRecord(t, a, &three)
	if err = EnrollActivities(i); err != nil {
		t.Fatal(err)
	}
	if err = i.EnableJournal(); err != nil {
		t.Fatal(err)
	}
	if err = CloseActivities(i); err != nil {
		t.Fatal(err)
	}
	ordered, err := i.FreezeJournal()
	if err != nil || !ordered {
		t.Fatalf("freeze=%t err=%v", ordered, err)
	}
	return i, a, ha, b, hb, []any{&one, &two, &three}
}
func TestJournalCursorExactOwnerOrderAndOneUse(t *testing.T) {
	i, a, ha, b, hb, want := journalFixture(t)
	for n, step := range []struct {
		o     *journalOwner
		h     Handle
		phase Operation
	}{{a, ha, LocalPreparation}, {b, hb, AllPreparation}, {a, ha, AllPreparation}} {
		if i.NextJournalOwner() != step.o {
			t.Fatalf("owner at cursor %d", n)
		}
		if err := step.h.Call(t.Context(), step.o, i, step.phase, nil); err != nil {
			t.Fatal(err)
		}
		p := step.o.permits[len(step.o.permits)-1]
		if _, err := RunRecords(step.o, p); !errors.Is(err, ErrDrainPermit) {
			t.Fatalf("stale range=%v", err)
		}
		if _, err := ConsumeDrain(step.o, p, step.phase, nil); !errors.Is(err, ErrDrainPermit) {
			t.Fatalf("replay=%v", err)
		}
	}
	if i.NextJournalOwner() != nil {
		t.Fatal("cursor not exhausted")
	}
	if !reflect.DeepEqual(a.executed, []any{want[0], want[2]}) || !reflect.DeepEqual(b.executed, []any{want[1]}) {
		t.Fatal("execution projections differ")
	}
	if _, err := i.ChildFrame(i.RootFrame(), "imperative", ""); !errors.Is(err, ErrJournal) {
		t.Fatalf("late frame=%v", err)
	}
	if err := AppendJournal(a, a.frame, new(int)); !errors.Is(err, ErrJournal) {
		t.Fatalf("late operation=%v", err)
	}
	if err := ha.Call(t.Context(), a, i, Completion, nil); err != nil {
		t.Fatal(err)
	}
}
func TestJournalSkippedReorderedAndForeignTokens(t *testing.T) {
	for _, kind := range []string{"skip", "reorder", "foreign-owner", "foreign-invocation", "copied-owner", "public-operation"} {
		t.Run(kind, func(t *testing.T) {
			i, a, ha, b, hb, _ := journalFixture(t)
			var err error
			switch kind {
			case "skip":
				err = ha.Call(t.Context(), a, i, Completion, nil)
			case "reorder":
				err = hb.Call(t.Context(), b, i, AllPreparation, nil)
			case "foreign-owner":
				err = ha.Call(t.Context(), b, i, AllPreparation, nil)
			case "foreign-invocation":
				err = ha.Call(t.Context(), a, NewInvocation(), AllPreparation, nil)
			case "copied-owner":
				copy := *a
				err = ha.Call(t.Context(), &copy, i, AllPreparation, nil)
			case "public-operation":
				err = ha.Call(t.Context(), a, i, PublicFlush, nil)
			}
			if err == nil || len(a.executed) != 0 || len(b.executed) != 0 {
				t.Fatalf("denied=%v", err)
			}
			if kind == "skip" || kind == "reorder" {
				if !errors.Is(ProtectedFailure(i), ErrJournal) {
					t.Fatal("cursor denial was not sticky")
				}
				if err = ha.Call(t.Context(), a, i, AllPreparation, nil); err == nil {
					t.Fatal("caught denial permitted suffix")
				}
			}
		})
	}
}
func TestJournalFailedRunCannotAdvanceOrReplay(t *testing.T) {
	i, a, ha, b, hb, _ := journalFixture(t)
	a.fail = true
	if err := ha.Call(t.Context(), a, i, AllPreparation, nil); err == nil {
		t.Fatal("run failure lost")
	}
	if err := ha.Call(t.Context(), a, i, AllPreparation, nil); err == nil {
		t.Fatal("failed run was replayed")
	}
	if i.NextJournalOwner() != a {
		t.Fatal("failed run advanced cursor")
	}
	// Native execution failure is latched by the engine, so no later run can be
	// selected even if a caller catches its preparation return.
	if err := hb.Call(t.Context(), b, i, AllPreparation, nil); err == nil {
		t.Fatal("suffix was selected")
	}
}
func TestJournalProjectionMismatchAndUnfinishedFrames(t *testing.T) {
	for _, kind := range []string{"missing", "duplicated", "unfinished", "wrong-frame"} {
		t.Run(kind, func(t *testing.T) {
			i := NewInvocation()
			root := i.RootFrame()
			a, _ := newJournalOwner(t, i, root)
			child, _ := i.ChildFrame(root, "imperative", "")
			b, _ := newJournalOwner(t, i, child)
			x, y := 1, 2
			appendJournalRecord(t, a, &x)
			appendJournalRecord(t, b, &y)
			if kind != "unfinished" {
				SealFrame(child)
			}
			if kind == "missing" {
				b.records = append(b.records, new(int))
			}
			if kind == "wrong-frame" {
				b.frames[&y] = root
			}
			if kind == "duplicated" {
				if err := AppendJournal(a, root, &x); err != nil {
					t.Fatal(err)
				}
			}
			if err := i.EnableJournal(); err != nil {
				t.Fatal(err)
			}
			ordered, err := i.FreezeJournal()
			if !ordered || !errors.Is(err, ErrJournal) {
				t.Fatalf("ordered=%t err=%v", ordered, err)
			}
			if _, again := i.FreezeJournal(); again == nil {
				t.Fatal("failed freeze became success")
			}
		})
	}
}
func TestJournalConcurrentBindingsUseDeclaredOrder(t *testing.T) {
	i := NewInvocation()
	root := i.RootFrame()
	a, _ := newJournalOwner(t, i, root)
	bFrame, _ := i.ChildFrame(root, "binding", "02")
	b, _ := newJournalOwner(t, i, bFrame)
	aFrame, _ := i.ChildFrame(root, "binding", "01")
	a.frame = aFrame
	var wg sync.WaitGroup
	wg.Add(2)
	x, y := 1, 2
	go func() { defer wg.Done(); appendJournalRecord(t, b, &y); SealFrame(bFrame) }()
	go func() { defer wg.Done(); appendJournalRecord(t, a, &x); SealFrame(aFrame) }()
	wg.Wait()
	if err := i.EnableJournal(); err != nil {
		t.Fatal(err)
	}
	if _, err := i.FreezeJournal(); err != nil {
		t.Fatal(err)
	}
	if i.NextJournalOwner() != a {
		t.Fatal("binding completion changed declared order")
	}
}
