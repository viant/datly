package drainowner

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type scopedGroupFixture struct {
	inv           *Invocation
	group         BindingGroup
	ctx           context.Context
	parent, child *int
	activity      Activity
	typ           reflect.Type
}

func newScopedGroupFixture(t *testing.T) *scopedGroupFixture {
	t.Helper()
	inv, g := openTestBindingGroup(t)
	p, c := new(int), new(int)
	typ := reflect.TypeFor[*struct{ Value int }]()
	d, e := NewScopedGroupTarget(p, c, "GET:/a", "GET:/leaf", "lazy", typ)
	if e != nil {
		t.Fatal(e)
	}
	ctx, e := g.Enter(t.Context(), "a")
	if e != nil {
		t.Fatal(e)
	}
	if e = ValidateScopedBindingGroupTarget(ctx, "GET:/a", p, d); e != nil {
		t.Fatal(e)
	}
	if e = BindingGroupTargetAdmission(ctx, inv, false); e != nil {
		t.Fatal(e)
	}
	if _, e = BindingGroupReadAdmission(ctx); !errors.Is(e, ErrBindingGroup) {
		t.Fatal("custom gained reader admission", e)
	}
	a, e := AdmitBindingGroupActivity(ctx, inv)
	if e != nil {
		t.Fatal(e)
	}
	return &scopedGroupFixture{inv, g, ctx, p, c, a, typ}
}
func (f *scopedGroupFixture) reserve(t *testing.T) (context.Context, BindingGroupScope, BindingGroupDispatch) {
	t.Helper()
	ctx, l, e := EnterBindingGroupScope(f.ctx, "lazy")
	if e != nil {
		t.Fatal(e)
	}
	ctx, d, e := PrepareBindingGroupDispatch(ctx, f.parent, "GET:/leaf", f.typ)
	if e != nil {
		t.Fatal(e)
	}
	return ctx, l, d
}
func (f *scopedGroupFixture) admit(t *testing.T, ctx context.Context) Activity {
	t.Helper()
	if e := ValidateRegisteredBindingGroupReader(ctx, "GET:/leaf", f.child); e != nil {
		t.Fatal(e)
	}
	if e := BindingGroupTargetAdmission(ctx, f.inv, true); e != nil {
		t.Fatal(e)
	}
	a, e := AdmitBindingGroupActivity(ctx, f.inv)
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func TestScopedGroupSeparateRetirementAndRepeatedScopeWithoutDispatch(t *testing.T) {
	f := newScopedGroupFixture(t)
	ctx, l, d := f.reserve(t)
	a := f.admit(t, ctx)
	if a.cell == f.activity.cell || a.cell.observation == f.activity.cell.observation {
		t.Fatal("shared activity or observation")
	}
	if e := FinishActivity(f.inv, a, nil); e != nil {
		t.Fatal(e)
	}
	if bindingContext(f.ctx).finished {
		t.Fatal("child retired parent")
	}
	if e := d.Complete(nil); e != nil {
		t.Fatal(e)
	}
	if e := d.Complete(nil); !errors.Is(e, ErrBindingGroup) {
		t.Fatal("duplicate provider close", e)
	}
	if e := FinishActivity(f.inv, a, nil); !errors.Is(e, ErrActivity) {
		t.Fatal("duplicate child retirement", e)
	}
	if e := l.Close(); e != nil {
		t.Fatal(e)
	}
	again, l2, e := EnterBindingGroupScope(f.ctx, "lazy")
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = PrepareBindingGroupDispatch(again, f.parent, "GET:/leaf", f.typ); !errors.Is(e, ErrBindingGroup) {
		t.Fatal("second dispatch", e)
	}
	if e = l2.Close(); e != nil {
		t.Fatal(e)
	}
	if e = FinishActivity(f.inv, f.activity, nil); e != nil {
		t.Fatal(e)
	}
	if _, _, e = EnterBindingGroupScope(f.ctx, "lazy"); !errors.Is(e, ErrBindingGroup) {
		t.Fatal("retained scope", e)
	}
	if e = f.group.Close(); e != nil {
		t.Fatal(e)
	}
	if e = CloseActivities(f.inv); e != nil {
		t.Fatal(e)
	}
}
func TestScopedGroupChildFailureRetainedAndParentWrapper(t *testing.T) {
	for _, mode := range []string{"caught", "wrapped", "early", "late", "same pointer"} {
		t.Run(mode, func(t *testing.T) {
			f := newScopedGroupFixture(t)
			ctx, l, d := f.reserve(t)
			cause := errors.New("child failure")
			if mode != "early" {
				a := f.admit(t, ctx)
				var retired error
				if mode != "late" {
					retired = cause
				}
				if e := FinishActivity(f.inv, a, retired); e != nil {
					t.Fatal(e)
				}
			}
			if e := d.Complete(cause); e != nil {
				t.Fatal(e)
			}
			if !errors.Is(ProtectedFailure(f.inv), cause) {
				t.Fatal("delayed child failure")
			}
			if bindingContext(f.ctx).finished {
				t.Fatal("child failure retired parent")
			}
			if e := l.Close(); e != nil {
				t.Fatal(e)
			}
			parentCause := error(nil)
			if mode == "wrapped" {
				parentCause = fmt.Errorf("source predicate wrapper: %w", cause)
			}
			if mode == "same pointer" {
				parentCause = cause
			}
			e := FinishActivity(f.inv, f.activity, parentCause)
			if parentCause == nil && !errors.Is(e, cause) {
				t.Fatal("caught failure accepted", e)
			}
			if parentCause != nil && e != nil {
				t.Fatal("source wrapper replaced", e)
			}
			if bindingContext(f.ctx).failure != parentCause && parentCause != nil {
				t.Fatal("source cause identity lost")
			}
			// An ordinary same-group sibling may still execute after nonterminal child failure.
			_, b := enterTestBindingRead(t, f.inv, f.group, "b")
			if e := FinishActivity(f.inv, b, nil); e != nil {
				t.Fatal(e)
			}
			if e := f.group.Close(); e != nil {
				t.Fatal(e)
			}
			if !errors.Is(CloseActivities(f.inv), cause) {
				t.Fatal("root cleared child failure")
			}
			ledger := &f.inv.identity.activities
			count := 0
			for _, failure := range ledger.entries {
				if errors.Is(failure.cause, cause) {
					count++
				}
			}
			if count != 2 {
				t.Fatalf("child/parent independent failures=%d", count)
			}
		})
	}
}
func TestScopedGroupDispatchAuthority(t *testing.T) {
	for _, mode := range []string{"no lease", "wrong parent", "wrong route", "wrong type", "closed lease", "cancel", "foreign issuer", "wrong registration", "replay validation"} {
		t.Run(mode, func(t *testing.T) {
			f := newScopedGroupFixture(t)
			ctx, l, e := EnterBindingGroupScope(f.ctx, "lazy")
			if e != nil {
				t.Fatal(e)
			}
			p, route, typ := f.parent, "GET:/leaf", f.typ
			switch mode {
			case "no lease":
				ctx = f.ctx
			case "wrong parent":
				p = new(int)
			case "wrong route":
				route = "GET:/else"
			case "wrong type":
				typ = reflect.TypeFor[int]()
			case "closed lease":
				if e = l.Close(); e != nil {
					t.Fatal(e)
				}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			child, d, e := PrepareBindingGroupDispatch(ctx, p, route, typ)
			switch mode {
			case "foreign issuer", "wrong registration", "replay validation":
				if e != nil {
					t.Fatal(e)
				}
				if mode == "wrong registration" {
					e = ValidateRegisteredBindingGroupReader(child, route, new(int))
				} else {
					if e = ValidateRegisteredBindingGroupReader(child, route, f.child); e != nil {
						t.Fatal(e)
					}
					if mode == "foreign issuer" {
						_, e = AdmitBindingGroupActivity(child, NewInvocation())
					} else {
						e = ValidateRegisteredBindingGroupReader(child, route, f.child)
					}
				}
				if !errors.Is(e, ErrBindingGroup) {
					t.Fatal("invalid child admitted", e)
				}
				if e = d.Complete(errors.New("admission denied")); e != nil {
					t.Fatal(e)
				}
			default:
				if e == nil || child != nil {
					t.Fatal("invalid dispatch admitted", e)
				}
			}
			if mode != "closed lease" {
				if e = l.Close(); e != nil {
					t.Fatal(e)
				}
			}
			_ = FinishActivity(f.inv, f.activity, errors.New("source failed"))
			_ = f.group.Close()
			_ = CloseActivities(f.inv)
		})
	}
}
func TestScopedGroupOutstandingChildTerminal(t *testing.T) {
	for _, admit := range []bool{false, true} {
		t.Run(fmt.Sprint(admit), func(t *testing.T) {
			f := newScopedGroupFixture(t)
			ctx, l, d := f.reserve(t)
			var a Activity
			if admit {
				a = f.admit(t, ctx)
			}
			if e := FinishActivity(f.inv, f.activity, nil); !errors.Is(e, ErrActivityUnfinished) {
				t.Fatal("parent left live child", e)
			}
			if e := l.Close(); !errors.Is(e, ErrBindingGroup) {
				t.Fatal("parent did not close scope", e)
			}
			if _, e := f.group.Enter(t.Context(), "b"); e == nil {
				t.Fatal("terminal unfinished child allowed sibling")
			}
			if admit {
				if e := FinishActivity(f.inv, a, context.Canceled); e != nil {
					t.Fatal(e)
				}
			}
			if e := d.Complete(context.Canceled); e != nil {
				t.Fatal(e)
			}
			if e := f.group.Close(); !errors.Is(e, ErrActivityUnfinished) {
				t.Fatal(e)
			}
			if !errors.Is(CloseActivities(f.inv), ErrActivityUnfinished) {
				t.Fatal("unfinished failure lost")
			}
		})
	}
}
