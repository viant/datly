package writer

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
)

type finiteQueueObservationHook struct {
	calls int
	run   func(context.Context, *phaseTestRoot) error
}

func (hook *finiteQueueObservationHook) AfterQueue(ctx context.Context, row *phaseTestRoot, _ h.LifecycleContext[phaseTestRoot, h.NoParent, phaseSelectionOutput]) error {
	hook.calls++
	if hook.run != nil {
		return hook.run(ctx, row)
	}
	return nil
}

func TestFiniteAfterQueueObservesWholeStateAndRetiresFailure(t *testing.T) {
	for _, mode := range []string{"healthy", "root", "child", "current", "output", "actions", "payload", "error", "panic", "validation-panic", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(parent context.Context) {
				ctx, cancel := context.WithCancel(parent)
				defer cancel()
				p, _, actions := projectedActionFixture(t, ctx, false, false)
				p.actions.Rows = append([]*Action(nil), actions...)
				hook := &finiteQueueObservationHook{run: func(_ context.Context, row *phaseTestRoot) error {
					switch mode {
					case "root":
						row.Enabled = !row.Enabled
					case "child":
						row.Children[0].Value = "changed"
					case "current":
						p.reconciliation.roots[0].frame.Previous.Interface().(*phaseTestRoot).Enabled = true
					case "output":
						p.output.(*phaseSelectionOutput).Status = "changed"
					case "actions":
						p.actions.Rows[1] = p.actions.Rows[0]
					case "payload":
						payload := actions[0].Entity.Interface().(*phaseTestRoot)
						payload.Enabled = !payload.Enabled
					case "error":
						return errors.New("hook failed")
					case "panic":
						panic("hook panic")
					case "validation-panic":
						p.metadata.InputField = 999
					case "cancel":
						cancel()
					}
					return nil
				}}
				for _, root := range p.reconciliation.roots {
					root.frame.Hook = reflect.ValueOf(hook)
				}
				var err error
				var panicked any
				func() {
					defer func() { panicked = recover() }()
					for _, root := range p.reconciliation.roots {
						if err = p.callEntityHook(ctx, "AfterQueue", root.frame); err != nil {
							return
						}
					}
				}()
				if mode == "healthy" {
					if err != nil || panicked != nil || hook.calls != 2 || !p.reconciliation.active {
						t.Fatal("healthy observation failed", err, panicked, hook.calls)
					}
				} else {
					if err == nil && panicked == nil || p.executionFailure == nil || p.reconciliation.active || hook.calls != 1 {
						t.Fatal("failed hook advanced or did not retire", err, panicked, hook.calls)
					}
					if (mode == "panic" || mode == "validation-panic") && panicked == nil {
						t.Fatal("panic propagation lost")
					}
				}
				// The dynamic observation closes even after errors and panics.
				finish, e := engine.BeginReconciliation(parent)
				if e != nil {
					t.Fatal("observation leaked", e)
				}
				if e = finish(); e != nil {
					t.Fatal(e)
				}
			})
		})
	}
}

func TestFiniteAfterQueueSwallowedMutationIsTerminal(t *testing.T) {
	db := sqlite.New(t)
	var p *Program
	var calls int
	_, err := engine.New().Execute(context.Background(), engine.Request{
		Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: db.DB},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
			var actions []*Action
			p, _, actions = projectedActionFixture(t, ctx, false, false)
			p.actions.Rows = actions
			hook := &finiteQueueObservationHook{run: func(call context.Context, _ *phaseTestRoot) error {
				calls++
				service, _, e := inv.Binder.Lookup(call, h.DMLKey)
				if e != nil {
					return e
				}
				if e = service.(h.DML).Insert("items", &phaseTestRoot{ID: 99}); e == nil {
					t.Fatal("observation admitted mutation")
				}
				return nil // Deliberately swallow the rejected effect.
			}}
			p.reconciliation.roots[0].frame.Hook = reflect.ValueOf(hook)
			if e := p.callEntityHook(ctx, "AfterQueue", p.reconciliation.roots[0].frame); e == nil {
				t.Fatal("swallowed effect accepted")
			}
			return nil, p.executionFailure
		}),
	})
	if err == nil || calls != 1 || p == nil || p.reconciliation.active || p.executionFailure == nil {
		t.Fatal("swallowed effect not retained", err, calls)
	}
}

func TestOrdinaryAfterQueueKeepsExistingMutationBehavior(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, _, _ := projectedActionFixture(t, ctx, false, false)
		p.phaseSelectionAttempted = false
		hook := &finiteQueueObservationHook{run: func(_ context.Context, row *phaseTestRoot) error { row.Enabled = false; return nil }}
		root := p.reconciliation.roots[0].frame
		root.Hook = reflect.ValueOf(hook)
		if err := p.callEntityHook(ctx, "AfterQueue", root); err != nil || root.Entity.Interface().(*phaseTestRoot).Enabled || hook.calls != 1 {
			t.Fatal("ordinary hook changed", err)
		}
	})
}
