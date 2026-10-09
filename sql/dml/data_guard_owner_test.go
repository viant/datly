package dml

import (
	"context"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness"
)

func TestBoundGuardExactNativeViews(t *testing.T) {
	db := testharness.NewSQLiteHarness(t)
	d := NewData(db.DB)
	if err := d.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	binding := drainowner.NewGuardBinding()
	calls := 0
	if err := d.RegisterBoundExecutionGuard(func(context.Context) error { calls++; return nil }, binding); err != nil {
		t.Fatal(err)
	}
	child := d.ComponentData(ComponentBinding, "a").(*Data)
	grandchild := child.ComponentData(ComponentImperative, "").(*Data)
	for _, view := range []*Data{d, child, grandchild} {
		if err := view.ValidateBoundExecutionGuard(binding); err != nil {
			t.Fatal("enrolled view rejected", err)
		}
	}
	copyRoot := &Data{drainOwner: d.drainOwner, open: true}
	copyChild := &Data{root: d, parent: d, open: true}
	foreign := NewData(db.DB)
	for _, view := range []*Data{nil, copyRoot, copyChild, foreign} {
		if err := view.ValidateBoundExecutionGuard(binding); err == nil {
			t.Fatal("foreign/copy accepted")
		}
	}
	copyBinding := *binding
	if err := d.ValidateBoundExecutionGuard(&copyBinding); err == nil {
		t.Fatal("copy binding accepted")
	}
	if err := d.ValidateBoundExecutionGuard(drainowner.NewGuardBinding()); err == nil {
		t.Fatal("unregistered binding accepted")
	}
	child.SealComponent()
	for _, view := range []*Data{child, grandchild} {
		if err := view.ValidateBoundExecutionGuard(binding); err == nil {
			t.Fatal("closed ancestor accepted")
		}
	}
	if calls != 0 || len(d.queue) != 0 || d.tx != nil {
		t.Fatal("identity check invoked callbacks or effects")
	}
	if err := d.CloseMutationAdmission(); err != nil {
		t.Fatal(err)
	}
	if err := d.ValidateBoundExecutionGuard(binding); err == nil {
		t.Fatal("closed owner accepted")
	}
}

func TestBoundGuardRejectsRetiredAndBrokenViewLinks(t *testing.T) {
	for _, mode := range []string{"completed", "failed", "wrong-parent", "cycle", "duplicate-membership"} {
		t.Run(mode, func(t *testing.T) {
			d := NewData(testharness.NewSQLiteHarness(t).DB)
			if err := d.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			binding := drainowner.NewGuardBinding()
			if err := d.RegisterBoundExecutionGuard(func(context.Context) error { return nil }, binding); err != nil {
				t.Fatal(err)
			}
			view := d.ComponentData(ComponentBinding, "a").(*Data)
			switch mode {
			case "completed":
				d.completed = true
			case "failed":
				d.failed = ErrInvocationFailed
			case "wrong-parent":
				view.parent = NewData(d.db)
			case "cycle":
				view.parent = view
			case "duplicate-membership":
				d.bindings = append(d.bindings, view)
			}
			if err := view.ValidateBoundExecutionGuard(binding); err == nil {
				t.Fatal("invalid native state accepted")
			}
		})
	}
}
