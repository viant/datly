package writer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

func TestFiniteSourceAttemptCannotCompleteWithoutProof(t *testing.T) {
	db := sqlite.New(t)
	var program *Program
	_, err := engine.New().Execute(context.Background(), engine.Request{
		Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: db.DB},
		Handler: &phaseSelectionEngineHandler{test: t, prepare: func(p *Program) { program = p }, execute: func(ctx context.Context, inv rhandler.Invocation, p *Program, phases *finiteSourcePhases, _ *phaseSelectionHook) (any, error) {
			if _, err := p.selectFinitePhasePlan(ctx, phases); err != nil {
				return nil, err
			}
			return nil, nil
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "completion proof unavailable") {
		t.Fatalf("unfinished source execution completed: %v", err)
	}
	if program == nil || program.executionFailure == nil || program.reconciliation.active {
		t.Fatal("completion failure was not retained")
	}
}

func TestFiniteSourceInProgressPurposeAndReplacedContext(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(map[bool]string{false: "native in-progress", true: "replaced context"}[replaced], func(t *testing.T) {
			db := sqlite.New(t)
			var checked bool
			_, err := engine.New().Execute(context.Background(), engine.Request{
				Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: db.DB},
				Handler: &phaseSelectionEngineHandler{test: t, prepare: func(p *Program) {}, execute: func(ctx context.Context, inv rhandler.Invocation, p *Program, phases *finiteSourcePhases, _ *phaseSelectionHook) (any, error) {
					if _, err := p.selectFinitePhasePlan(ctx, phases); err != nil {
						return nil, err
					}
					service, _, err := inv.Binder.Lookup(ctx, xhandler.DMLKey)
					if err != nil {
						return nil, err
					}
					if err = service.(interface{ ValidateExecutionGuards(context.Context) error }).ValidateExecutionGuards(ctx); err != nil {
						t.Fatal("native in-progress failed", err)
					}
					checked = true
					if replaced {
						if _, err := drainowner.GuardPurpose(context.Background(), p.guardBinding); err == nil {
							t.Fatal("replaced context authorized")
						}
					}
					return nil, nil
				}},
			})
			if !checked || err == nil || !strings.Contains(err.Error(), "completion proof unavailable") {
				t.Fatalf("checked=%v completion=%v", checked, err)
			}
		})
	}
}

func TestFiniteRetainedPayloadGuardBeforeReady(t *testing.T) {
	for _, mode := range []string{"duplicate-slot", "clear-registry", "payload", "removed-token", "replaced-context", "foreign-context", "expired-context", "validation-panic"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, _, _ := projectedActionFixture(t, ctx, true, false)
				handler := &Handler{metadata: p.metadata, inputType: reflect.TypeFor[phaseOccurrenceInput]()}
				inv := rhandler.Invocation{Input: p.input, Snapshot: p, Binder: &qcBinder{}}
				check, err := handler.CapturedExecutionGuard(inv)
				if err != nil {
					t.Fatal(err)
				}
				binding, err := handler.CapturedExecutionGuardBinding(inv)
				if err != nil {
					t.Fatal(err)
				}
				db := sqlite.New(t)
				data := dml.NewData(db.DB)
				if err = data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				var retained context.Context
				if err = data.RegisterBoundExecutionGuard(func(call context.Context) error { retained = call; return check(call) }, binding); err != nil {
					t.Fatal(err)
				}
				if p.executionGuardReady {
					t.Fatal("fixture unexpectedly ready")
				}
				if err = data.ValidateExecutionGuards(ctx); err != nil {
					t.Fatal("healthy in-progress projection", err)
				}
				switch mode {
				case "duplicate-slot":
					p.reconciliation.projectedActions[1] = p.reconciliation.projectedActions[0]
				case "clear-registry":
					p.reconciliation.projectedActions = nil
					p.reconciliation.projectedAuthorities = nil
					p.reconciliation.roots = nil
					p.reconciliation.rootProjections = nil
				case "payload":
					p.reconciliation.projectedActions[0].Entity.Interface().(*phaseTestRoot).Enabled = true
				case "removed-token":
					p.reconciliation.projectedActions[0].projected = nil
				case "validation-panic":
					p.metadata.InputField = 999
				case "replaced-context":
					err = check(context.Background())
				case "foreign-context", "expired-context":
					other := dml.NewData(nil)
					if e := other.BeginInvocation(); e != nil {
						t.Fatal(e)
					}
					otherBinding := drainowner.NewGuardBinding()
					if e := other.RegisterBoundExecutionGuard(func(context.Context) error { return nil }, otherBinding); e != nil {
						t.Fatal(e)
					}
					scopedBinding, scopedOwner := binding, data
					if mode == "foreign-context" {
						scopedBinding, scopedOwner = otherBinding, other
					}
					borrowed, close, e := drainowner.WithGuardPurpose(ctx, scopedOwner, scopedBinding, false)
					if e != nil {
						t.Fatal(e)
					}
					defer close()
					if mode == "expired-context" {
						close()
					}
					err = check(borrowed)
				}
				if mode != "replaced-context" && mode != "foreign-context" && mode != "expired-context" {
					err = data.ValidateExecutionGuards(ctx)
				}
				if err == nil || p.executionFailure == nil || p.reconciliation.active {
					t.Fatal("tampered unfinished program accepted", err)
				}
				if _, scopeErr := drainowner.GuardPurpose(retained, binding); !errors.Is(scopeErr, drainowner.ErrGuardScope) {
					t.Fatal("callback scope survived", scopeErr)
				}
				if err = data.Complete(ctx, nil); err == nil {
					t.Fatal("caught guard failure completed")
				}
			})
		})
	}
}

func TestFiniteSourceUnsupportedBindingStopsBeforeSelection(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, phases, hook := phaseSelectionFixture(t)
		handler := &Handler{metadata: p.metadata, inputType: reflect.TypeFor[phaseOccurrenceInput]()}
		inv := rhandler.Invocation{Input: p.input, Snapshot: p, Binder: &qcBinder{}}
		check, err := handler.CapturedExecutionGuard(inv)
		if err != nil {
			t.Fatal(err)
		}
		data := dml.NewData(nil)
		if err = data.BeginInvocation(); err != nil {
			t.Fatal(err)
		}
		// An ordinary-only forwarding owner cannot bind the Program's registration.
		if err = data.RegisterExecutionGuard(check); err != nil {
			t.Fatal(err)
		}
		if err = handler.CapturedExecutionGuardRegistered(inv); err != nil {
			t.Fatal(err)
		}
		if _, err = p.selectFinitePhasePlan(ctx, phases); err == nil || !strings.Contains(err.Error(), "bound native guard registration") {
			t.Fatal("unsupported binding admitted source effects", err)
		}
		if hook.calls != 0 || p.reconciliation != nil || p.executionFailure == nil {
			t.Fatal("selection or preparation ran before registration gate")
		}
		if err = data.Complete(ctx, nil); err == nil {
			t.Fatal("unsupported binding failure completed")
		}
	})
}
