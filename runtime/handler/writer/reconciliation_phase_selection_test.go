package writer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type phaseSelectionOutput struct {
	Data   []*phaseTestRoot
	Status string
}
type phaseSelectionHook struct {
	p      *Program
	phases *finiteSourcePhases
	native ReconciliationContext
	value  string
	calls  int
	run    func(context.Context, *phaseOccurrenceInput, *phaseSelectionOutput) error
	plan   ReconciliationPlan
}

func (h *phaseSelectionHook) ReconcileInput(ctx context.Context, in *phaseOccurrenceInput, out *phaseSelectionOutput, native ReconciliationContext) (ReconciliationPlan, error) {
	h.calls++
	h.native = native
	observations, err := native.Roots()
	if err != nil {
		return ReconciliationPlan{}, err
	}
	for _, root := range observations {
		root.Row.(*phaseTestRoot).Enabled = true
	}
	plan := ReconciliationPlan{}
	for _, root := range h.p.reconciliation.roots {
		plan.Roots = append(plan.Roots, ReconciliationRootPlan{Root: root.ref})
	}
	for _, phase := range h.phases.update {
		entry := ReconciliationPhasePlan{Phase: phase.name}
		for _, root := range h.p.reconciliation.roots {
			selected := ReconciliationRootPhasePlan{Root: root.ref}
			if phase.name == "children" {
				selected.Selected = []ReconciliationSelection{{Occurrence: root.roles[0].working[0], Assignments: []ReconciliationAssignment{{Field: "Value", Value: "planned"}, {Field: "Pointer", Value: &h.value}}}}
			}
			entry.Roots = append(entry.Roots, selected)
		}
		plan.Phases = append(plan.Phases, entry)
	}
	h.plan = plan
	if h.run != nil {
		if err = h.run(ctx, in, out); err != nil {
			return plan, err
		}
	}
	return h.plan, nil
}
func phaseSelectionFixture(t *testing.T) (*Program, *finiteSourcePhases, *phaseSelectionHook) {
	p, phases, _ := phaseOccurrenceFixture(t)
	p.actions = &MutationActions{}
	childHook := &phaseSelectionChildHook{}
	p.hooksByRecord = map[*Record]reflect.Value{}
	for _, rel := range p.metadata.Root.Relations {
		rel.Child.HookType = reflect.TypeFor[phaseSelectionChildHook]()
		p.hooksByRecord[rel.Child] = reflect.ValueOf(childHook)
	}
	for _, frame := range p.frames.Rows {
		if frame.Parent != nil {
			frame.Hook = reflect.ValueOf(childHook)
		}
	}
	p.output = &phaseSelectionOutput{}
	h := &phaseSelectionHook{p: p, phases: phases, value: "captured"}
	p.hook = reflect.ValueOf(h)
	return p, phases, h
}
func TestFinitePhaseSelectionSealsDetachedValuesAndClosesObservation(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, phases, h := phaseSelectionFixture(t)
		sealed, err := p.selectFinitePhasePlan(ctx, phases)
		if err != nil {
			t.Fatal(err)
		}
		for _, hook := range p.hooksByRecord {
			if hook.Interface().(*phaseSelectionChildHook).calls != 0 {
				t.Fatal("child Init called before phase reached")
			}
		}
		if h.calls != 1 || sealed != p.reconciliation.selectionSealed || !p.reconciliation.active {
			t.Fatal("selection authority not retained")
		}
		h.value = "later"
		h.plan.Phases[1].Roots[0].Selected[0].Assignments[0].Value = "later"
		if sealed.phases[1].roots[0].selected[0].Assignments[0].Value != "planned" || *sealed.phases[1].roots[0].selected[0].Assignments[1].Value.(*string) != "captured" {
			t.Fatal("hook storage retained")
		}
		for _, row := range p.input.(*phaseOccurrenceInput).Rows {
			if row.Enabled || row.Children[0].ID != 0 || row.Children[0].Value == "planned" {
				t.Fatal("selection changed live data")
			}
		}
		for _, allocation := range p.reconciliation.allocations {
			if allocation.allocated.IsValid() {
				t.Fatal("selection allocated")
			}
		}
		if _, err = h.native.Roots(); err == nil {
			t.Fatal("retained callback observation remained open")
		}
		if _, err = p.selectFinitePhasePlan(ctx, phases); err == nil || p.executionFailure == nil || p.reconciliation.active {
			t.Fatal("selection replay permitted")
		}
	})
}
func TestFinitePhaseSelectionRetiresFailuresAndRetainsCompletionFailure(t *testing.T) {
	for _, mode := range []string{"body", "parameter", "current", "output", "has", "topology", "hook-error", "panic", "cancel", "invalid-plan", "no-hook", "preparation"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, phases, h := phaseSelectionFixture(t)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				h.run = func(_ context.Context, in *phaseOccurrenceInput, out *phaseSelectionOutput) error {
					switch mode {
					case "body":
						in.Rows[0].Children[0].Value = "illegal"
					case "parameter":
						in.Parameter = "illegal"
					case "current":
						p.database.ByRecord[p.metadata.Root.Relations[0].Child][0].Interface().(*phaseTestChild).Value = "illegal"
					case "output":
						out.Status = "illegal"
					case "has":
						in.Rows[0].Has.ID = false
					case "topology":
						in.Rows[0].Children = nil
					case "hook-error":
						return errors.New("hook rejected")
					case "panic":
						panic("hook panic")
					case "cancel":
						cancel()
					case "invalid-plan":
						h.plan.Phases = nil
					}
					return nil
				}
				if mode == "no-hook" {
					p.hook = reflect.Value{}
				}
				if mode == "preparation" {
					p.frames.Rows[0].holderPosition = 99
				}
				var sealed *finitePhasePlan
				var err error
				var recovered any
				func() { defer func() { recovered = recover() }(); sealed, err = p.selectFinitePhasePlan(ctx, phases) }()
				if mode == "panic" {
					if recovered != "hook panic" {
						t.Fatalf("panic=%v", recovered)
					}
				} else if err == nil {
					t.Fatal("failure accepted")
				}
				if sealed != nil || p.executionFailure == nil {
					t.Fatal("failed selection can complete")
				}
				if p.reconciliation != nil && (p.reconciliation.active || p.reconciliation.selectionSealed != nil) {
					t.Fatal("failed authority remained usable")
				}
				if _, err = p.selectFinitePhasePlan(ctx, phases); err == nil || !strings.Contains(err.Error(), "already attempted") {
					t.Fatal("failed selection retried", err)
				}
			})
		})
	}
}

func TestFinitePhaseSelectionSwallowedFailureCannotCompleteEngine(t *testing.T) {
	for _, mode := range []string{"hook-error", "write", "allocate", "query", "query-row", "exec-read", "component-reader", "component-writer"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			connectors := &dsql.SQLComponent{}
			if err := connectors.RegisterConnector("owned", db.DB); err != nil {
				t.Fatal(err)
			}
			var p *Program
			_, err := engine.New().Execute(context.Background(), engine.Request{
				Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: db.DB},
				Capabilities: rhandler.InvocationCapabilities{Connector: connectors},
				Handler: &phaseSelectionEngineHandler{test: t, prepare: func(program *Program) { p = program }, execute: func(ctx context.Context, inv rhandler.Invocation, prepared *Program, compiled *finiteSourcePhases, preparedHook *phaseSelectionHook) (any, error) {
					var phases *finiteSourcePhases
					var hook *phaseSelectionHook
					p, phases, hook = prepared, compiled, preparedHook
					value, found, e := inv.Binder.Lookup(ctx, xhandler.DMLKey)
					if e != nil || !found {
						return nil, errors.New("missing native DML")
					}
					seq, found, e := inv.Binder.Lookup(ctx, xhandler.SequencerKey)
					if e != nil || !found {
						return nil, errors.New("missing native allocator")
					}
					sqlValue, found, e := inv.Binder.Lookup(ctx, rhandler.TransactionSQLCapabilityKey)
					if e != nil || !found {
						return nil, errors.New("missing native transaction SQL")
					}
					tx, e := sqlValue.(rhandler.TransactionSQLProvider).Connector(ctx, "owned")
					if e != nil {
						return nil, e
					}
					// Same retained authority and admission gate used by Runtime's
					// scoped component invoker; no target is executed on denial.
					retain := engine.RetainMutationAuthority(ctx)
					hook.run = func(ctx context.Context, in *phaseOccurrenceInput, _ *phaseSelectionOutput) error {
						switch mode {
						case "hook-error":
							return errors.New("selection sentinel")
						case "write":
							_ = value.(xhandler.DML).Insert("no_selection_writes", in.Rows)
						case "allocate":
							_ = seq.(xhandler.Sequencer).Allocate(ctx, "no_selection_allocations", in.Rows, "ID")
						case "query":
							rows, _ := tx.QueryContext(context.Background(), "SELECT 1")
							if rows != nil {
								rows.Close()
							}
						case "query-row":
							var n int
							_ = tx.QueryRowContext(context.Background(), "SELECT 1").Scan(&n)
						case "exec-read":
							_, _ = tx.ExecContext(context.Background(), "SELECT 1")
						case "component-reader", "component-writer":
							_ = engine.CheckComponentMutation(retain(context.Background()), mode == "component-reader")
						}
						return nil
					}
					sealed, e := p.selectFinitePhasePlan(ctx, phases)
					if e == nil || sealed != nil {
						return nil, errors.New("selection failure accepted")
					}
					// Deliberately swallow the result; native completion must retain it.
					return nil, nil
				}},
			})
			if err == nil || p == nil || p.executionFailure == nil {
				t.Fatal("swallowed selection failure completed", err)
			}
			if mode != "hook-error" && !errors.Is(p.executionFailure, engine.ErrWriteEligibilityMutation) {
				t.Fatal("managed guard violation was not retained", err)
			}
			if mode == "hook-error" && !strings.Contains(err.Error(), "selection sentinel") {
				t.Fatal("native completion lost retained cause", err)
			}
		})
	}
}

type phaseSelectionEngineHandler struct {
	test    *testing.T
	p       *Program
	phases  *finiteSourcePhases
	hook    *phaseSelectionHook
	handler *Handler
	prepare func(*Program)
	execute func(context.Context, rhandler.Invocation, *Program, *finiteSourcePhases, *phaseSelectionHook) (any, error)
}

func (h *phaseSelectionEngineHandler) owned(inv rhandler.Invocation) rhandler.Invocation {
	inv.Input = h.p.input
	inv.Snapshot = h.p
	return inv
}
func (h *phaseSelectionEngineHandler) CapturedExecutionGuard(inv rhandler.Invocation) (func(context.Context) error, error) {
	h.p, h.phases, h.hook = phaseSelectionFixture(h.test)
	h.prepare(h.p)
	h.handler = &Handler{metadata: h.p.metadata, inputType: reflect.TypeFor[phaseOccurrenceInput]()}
	return h.handler.CapturedExecutionGuard(h.owned(inv))
}
func (h *phaseSelectionEngineHandler) CapturedExecutionGuardRegistered(inv rhandler.Invocation) error {
	return h.handler.CapturedExecutionGuardRegistered(h.owned(inv))
}
func (h *phaseSelectionEngineHandler) Execute(ctx context.Context, inv rhandler.Invocation) (any, error) {
	return h.execute(ctx, inv, h.p, h.phases, h.hook)
}

type phaseSelectionChildHook struct{ calls int }

func (h *phaseSelectionChildHook) Init(context.Context, *phaseTestChild) error { h.calls++; return nil }
