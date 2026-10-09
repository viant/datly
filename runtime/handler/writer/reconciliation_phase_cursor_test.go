package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
)

func TestFiniteCursorCompiledWorkflowBoundaries(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, c, raw := finitePhasePlanFixture(t, ctx)
		plan, e := p.sealFinitePhasePlan(ctx, c, raw)
		if e != nil {
			t.Fatal(e)
		}
		first := &plan.phases[0]
		first.phase.workflow = "current-deletes-then-working-inserts"
		for i, root := range p.reconciliation.roots {
			first.roots[i].deletes = append([]OccurrenceRef(nil), root.roles[1].current...)
		}
		// Use Children Current for a separate synthetic canonical schedule test,
		// preserving native source enumeration rather than flattening parents.
		first.phase.role = p.metadata.Root.Relations[0]
		first.current = nil
		for i, root := range p.reconciliation.roots {
			first.roots[i].deletes = append([]OccurrenceRef(nil), root.roles[0].current...)
			first.current = append(first.current, first.roots[i].deletes...)
		}
		first.current, e = p.reconciliation.orderedPhaseCurrent(first.phase.role.Child, first.current)
		if e != nil {
			t.Fatal(e)
		}
		last := &plan.phases[1]
		for i := range last.roots {
			last.roots[i].followup = []ReconciliationAssignment{{Field: "Enabled", Value: false, MarkPresent: true}}
		}
		steps, e := finitePhaseSegments(plan)
		if e != nil {
			t.Fatal(e)
		}
		var order []string
		for _, s := range steps {
			order = append(order, fmt.Sprintf("%d/%d/%s", s.phase, s.root, s.kind))
		}
		want := []string{"0/-1/deletes", "0/-1/inserts", "1/0/updates", "1/0/deletes", "1/0/inserts", "1/0/followup-values", "1/0/followup-admission", "1/1/updates", "1/1/deletes", "1/1/inserts", "1/1/followup-values", "1/1/followup-admission"}
		if !reflect.DeepEqual(order, want) {
			t.Fatal(order)
		}
		for i, ref := range steps[0].occurrences {
			if ref.ticket.currentOrdinal != i {
				t.Fatal("global Current order lost")
			}
		}
		last.phase.followup.placement = "after-phase"
		steps, e = finitePhaseSegments(plan)
		if e != nil {
			t.Fatal(e)
		}
		if steps[len(steps)-2].kind != "followup-admission" || steps[len(steps)-2].root != 0 || steps[len(steps)-1].root != 1 {
			t.Fatal("deferred followups interleaved with parent groups")
		}
		// Empty child selections still retain each group's boundary and setter.
		for i := range last.roots {
			last.roots[i].selected = nil
			last.roots[i].deletes = nil
		}
		empty, e := finitePhaseSegments(plan)
		if e != nil || len(empty) != len(steps) {
			t.Fatal("empty group dropped", e)
		}
		// The schedule owns copied slices, not an alias of mutable hook storage.
		if len(first.current) > 0 {
			old := steps[0].occurrences[0]
			first.current[0] = OccurrenceRef{}
			if steps[0].occurrences[0] != old {
				t.Fatal("schedule aliases plan refs")
			}
		}
	})
}

func TestFiniteCursorAllWorkflowsAndEmptyRootPopulation(t *testing.T) {
	cases := map[string][]string{
		"current-deletes":                              {"deletes"},
		"working-inserts":                              {"inserts"},
		"current-deletes-then-working-inserts":         {"deletes", "inserts"},
		"working-updates-then-inserts":                 {"updates", "inserts"},
		"working-updates-current-deletes-then-inserts": {"updates", "deletes", "inserts"},
	}
	for workflow, want := range cases {
		plan := &finitePhasePlan{owner: &reconciliationAttempt{}, compiled: &finiteSourcePhases{}, phases: []finitePhaseSelections{{phase: &finiteSourcePhase{scope: "all-roots", workflow: workflow}}}}
		steps, err := finitePhaseSegments(plan)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, step := range steps {
			got = append(got, step.kind)
			if step.root != -1 || len(step.occurrences) != 0 {
				t.Fatal("empty aggregate authority changed")
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal(workflow, got)
		}
		plan.phases[0].phase.scope = "each-root"
		steps, err = finitePhaseSegments(plan)
		if err != nil || len(steps) != 0 {
			t.Fatal("empty parent population invented a group", err)
		}
	}
}

func TestFiniteCursorActualEngineCheckpointAndTerminal(t *testing.T) {
	for _, mode := range []string{"next", "replay", "live", "current", "original", "output", "plan", "schedule", "position", "ticket", "foreign", "cancel", "terminal", "cursor-copy", "clear-ticket", "panic-next", "panic-validate", "before-live", "before-current", "before-output"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			if e := db.ExecStatements(context.Background(), "CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, Enabled INTEGER)", "INSERT INTO items(id,Enabled) VALUES(1,0),(2,0)"); e != nil {
				t.Fatal(e)
			}
			native := dml.NewData(db.DB)
			abort := errors.New("explicit cursor abort")
			var program *Program
			_, err := engine.New().Execute(context.Background(), engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: finiteAdmissionSource{native}, Handler: &phaseSelectionEngineHandler{test: t, prepare: func(p *Program) {
				program = p
				configureRootPreparation(p, p.hook.Interface().(*phaseSelectionHook))
				p.metadata.Root.QueueContract = "source-slice"
			}, execute: func(ctx context.Context, inv rh.Invocation, p *Program, c *finiteSourcePhases, _ *phaseSelectionHook) (any, error) {
				plan, e := p.selectFinitePhasePlan(ctx, c)
				if e != nil {
					return nil, e
				}
				if e = p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); e != nil {
					return nil, e
				}
				seq, _, e := inv.Binder.Lookup(ctx, h.SequencerKey)
				if e != nil {
					return nil, e
				}
				if e = p.allocateFinitePhaseRoots(ctx, plan, seq.(h.Sequencer)); e != nil {
					return nil, e
				}
				if e = p.projectFinitePhaseRoots(ctx, plan); e != nil {
					return nil, e
				}
				if e = p.admitFinitePhaseRoots(ctx, plan); e != nil {
					return nil, e
				}
				before, e := p.finiteAllocationClassificationState()
				if e != nil {
					return nil, e
				}

				if strings.HasPrefix(mode, "before-") {
					switch mode {
					case "before-live":
						p.input.(*phaseOccurrenceInput).Rows[1].Children[0].Value = "changed before cursor"
					case "before-current":
						p.database.ByRecord[p.metadata.Root.Relations[0].Child][0].Interface().(*phaseTestChild).Value = "changed before cursor"
					case "before-output":
						p.output.(*phaseSelectionOutput).Status = "changed before cursor"
					}
					e = p.startFinitePhaseCursor(ctx, plan)
					if e == nil || !p.finiteCursorAttempted || p.finiteCursorPublished || p.reconciliation.active || p.executionFailure == nil {
						t.Fatal("changed predecessor was recaptured", e)
					}
					return nil, nil
				}
				if e = p.startFinitePhaseCursor(ctx, plan); e != nil {
					return nil, e
				}
				ticket, e := p.nextFinitePhaseTransition(ctx)
				if e != nil || ticket == nil {
					t.Fatal("missing next", e)
				}
				if e = p.validateFinitePhaseTransition(ctx, ticket); e != nil {
					t.Fatal(e)
				}
				after, e := p.finiteAllocationClassificationState()
				if e != nil || before != after {
					t.Fatal("cursor mutated business data or actions", e)
				}
				verifyNativeRootAdmissionQueue(t, native, p, 2, false)
				if p.executionGuardReady || p.finiteCursor.position != 0 || p.reconciliation.allocations[p.reconciliation.roots[0].roles[0].working[0].ticket.frame].allocated.IsValid() {
					t.Fatal("cursor advanced/allocated/completed")
				}
				checkctx := ctx
				switch mode {
				case "next":
					again, e := p.nextFinitePhaseTransition(ctx)
					if e != nil || again != ticket {
						t.Fatal("next observation advanced", e)
					}
					return nil, abort
				case "terminal":
					return nil, nil
				case "replay":
					e = p.startFinitePhaseCursor(ctx, plan)
				case "live":
					p.input.(*phaseOccurrenceInput).Rows[1].Children[0].Value = "illegal"
				case "current":
					p.database.ByRecord[p.metadata.Root.Relations[0].Child][0].Interface().(*phaseTestChild).Value = "illegal"
				case "original":
					p.reconciliation.allocations[p.reconciliation.roots[0].frame].preallocation.Interface().(*phaseTestRoot).Enabled = true
				case "output":
					p.output.(*phaseSelectionOutput).Status = "illegal"
				case "plan":
					plan.phases[1].roots[0].selected[0].Assignments[0].Value = "illegal"
				case "schedule":
					p.finiteCursor.steps[0].kind = "inserts"
				case "position":
					p.finiteCursor.position = 1
				case "ticket":
					ticket.predecessor = "foreign"
				case "foreign":
					ticket = &finitePhaseTransition{cursor: p.finiteCursor, position: 0, predecessor: p.finiteCursor.checkpoint}

				case "cursor-copy":
					copy := *p.finiteCursor
					p.finiteCursor = &copy
				case "clear-ticket":
					p.finiteCursor.next = nil
				case "panic-next", "panic-validate":
					p.metadata.OutputField = 99
					var v any
					func() {
						defer func() { v = recover() }()
						if mode == "panic-next" {
							_, _ = p.nextFinitePhaseTransition(ctx)
						} else {
							_ = p.validateFinitePhaseTransition(ctx, ticket)
						}
					}()
					if v == nil || p.reconciliation.active || p.executionFailure == nil {
						t.Fatal("cursor panic did not retire")
					}
					return nil, nil
				case "cancel":
					var cancel context.CancelFunc
					checkctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				if mode != "replay" {
					e = p.validateFinitePhaseTransition(checkctx, ticket)
				}
				if e == nil || p.reconciliation.active || p.executionFailure == nil {
					t.Fatal("invalid transition accepted", mode, e)
				}
				// Even a swallowed error must fail the actual owner completion.
				return nil, nil
			}}})
			if mode == "next" {
				if !errors.Is(err, abort) {
					t.Fatal(err)
				}
			} else if err == nil || program.executionFailure == nil {
				t.Fatal("unfinished or failed cursor completed", mode, err)
			}
			if mode == "terminal" && !strings.Contains(err.Error(), "completion proof unavailable") {
				t.Fatal(err)
			}
			var count int
			if e := db.DB.QueryRow("SELECT COUNT(*) FROM items WHERE Enabled=0").Scan(&count); e != nil || count != 2 {
				t.Fatal("cursor drained or changed product rows", count, e)
			}
		})
	}
}
