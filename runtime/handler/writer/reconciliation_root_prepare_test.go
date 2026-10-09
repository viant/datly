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

type phaseRootPreparationHook struct {
	p               *Program
	calls           []int
	seenAssignments []bool
	alias           *string
	run             func(context.Context, *phaseTestRoot, string) error
}

func (hook *phaseRootPreparationHook) Init(ctx context.Context, row *phaseTestRoot, _ h.LifecycleContext[phaseTestRoot, h.NoParent, phaseSelectionOutput]) error {
	hook.calls = append(hook.calls, row.ID)
	hook.seenAssignments = append(hook.seenAssignments, row.Enabled)
	row.Enabled = true
	row.Has.Enabled = true
	if hook.run != nil {
		return hook.run(ctx, row, "Init")
	}
	return nil
}
func (hook *phaseRootPreparationHook) Validate(ctx context.Context, row *phaseTestRoot, _ h.LifecycleContext[phaseTestRoot, h.NoParent, phaseSelectionOutput]) error {
	if hook.run != nil {
		return hook.run(ctx, row, "Validate")
	}
	return nil
}

func configureRootPreparation(p *Program, selection *phaseSelectionHook) *phaseRootPreparationHook {
	probe := &phaseRootPreparationHook{p: p}
	for _, frame := range p.frames.Rows {
		frame.Fields = livePresence(frame.Record, frame.Entity.Elem())
		if frame.Parent == nil {
			frame.Action = p.finiteRootDecision.action
			frame.Hook = reflect.ValueOf(probe)
			current := &phaseTestRoot{ID: frame.Entity.Interface().(*phaseTestRoot).ID}
			frame.Previous = reflect.ValueOf(current)
			p.database.ByRecord[p.metadata.Root] = append(p.database.ByRecord[p.metadata.Root], frame.Previous)
		}
	}
	selection.run = func(_ context.Context, _ *phaseOccurrenceInput, _ *phaseSelectionOutput) error {
		for i := range selection.plan.Roots {
			selection.plan.Roots[i].Assignments = []ReconciliationAssignment{{Field: "Enabled", Value: false}}
		}
		return nil
	}
	return probe
}

func rootPreparationFixture(t *testing.T, ctx context.Context, setup ...func(*Program, *phaseRootPreparationHook)) (*Program, *finitePhasePlan, *phaseRootPreparationHook) {
	t.Helper()
	p, compiled, selection := phaseSelectionFixture(t)
	probe := configureRootPreparation(p, selection)
	for _, change := range setup {
		change(p, probe)
	}
	plan, err := p.selectFinitePhasePlan(ctx, compiled)
	if err != nil {
		t.Fatal(err)
	}
	return p, plan, probe
}

func TestFiniteRootPreparationDefaultsOnlyRootsAndRejectsReplay(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan, hook := rootPreparationFixture(t, ctx)
		probe := &subsetValidationProbe{}
		if err := p.prepareFinitePhaseRoots(ctx, plan, probe); err != nil {
			t.Fatal(err)
		}
		if !p.reconciliation.rootPrepared || !reflect.DeepEqual(hook.calls, []int{1, 2}) || len(probe.rows) != 1 {
			t.Fatal("root prefix incomplete", hook.calls)
		}
		for _, root := range p.reconciliation.roots {
			row := root.frame.Entity.Interface().(*phaseTestRoot)
			if !row.Enabled || !row.Has.Enabled || row.Children[0].Value != "new-"+map[int]string{1: "a", 2: "b"}[row.ID] || row.Children[0].ID != 0 {
				t.Fatal("root defaults or child protection lost")
			}
		}
		for _, allocation := range p.reconciliation.allocations {
			if allocation.allocated.IsValid() {
				t.Fatal("allocation before root readiness")
			}
		}
		for _, hook := range p.hooksByRecord {
			if hook.Interface().(*phaseSelectionChildHook).calls != 0 {
				t.Fatal("child Init before reached phase")
			}
		}
		if err := p.prepareFinitePhaseRoots(ctx, plan, probe); err == nil || p.reconciliation.active || p.reconciliation.rootPrepared || p.executionFailure == nil {
			t.Fatal("root preparation replay completed")
		}
	})
}

func TestFiniteRootPreparationFailureDomains(t *testing.T) {
	for _, mode := range []string{"identity", "identity-marker", "slot", "child", "child-marker", "shared-pointee", "current", "previous", "parameter", "output", "allocation", "original", "error", "panic", "cancel", "second-root-error", "custom-validation"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan, hook := rootPreparationFixture(t, ctx, func(p *Program, hook *phaseRootPreparationHook) {
					if mode == "shared-pointee" {
						value := "protected"
						hook.alias = &value
						p.input.(*phaseOccurrenceInput).Rows[0].Children[0].Pointer = hook.alias
					}
				})
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				probe := &subsetValidationProbe{}
				hook.run = func(_ context.Context, row *phaseTestRoot, stage string) error {
					if mode == "custom-validation" {
						if stage == "Validate" {
							return errors.New("custom validation failed")
						}
						return nil
					}
					if stage != "Init" {
						return nil
					}
					if mode == "second-root-error" {
						if row.ID == 2 {
							return errors.New("second root failed")
						}
						return nil
					}
					switch mode {
					case "identity":
						row.ID = 99
					case "identity-marker":
						row.Has.ID = false
					case "slot":
						p.input.(*phaseOccurrenceInput).Rows[0] = &phaseTestRoot{ID: 1, Has: &phaseTestHas{ID: true}}
					case "child":
						row.Children[0].Value = "changed"
					case "child-marker":
						row.Children[0].Has = &phaseTestHas{ID: true}
					case "shared-pointee":
						*hook.alias = "changed"
					case "current":
						p.database.ByRecord[p.metadata.Root.Relations[0].Child][0].Interface().(*phaseTestChild).Value = "changed"
					case "previous":
						p.reconciliation.roots[0].frame.Previous.Interface().(*phaseTestRoot).Enabled = true
					case "parameter":
						p.input.(*phaseOccurrenceInput).Parameter = "changed"
					case "output":
						p.output.(*phaseSelectionOutput).Status = "changed"
					case "allocation":
						p.reconciliation.allocations[p.frames.Rows[1]].allocated = reflect.ValueOf(row.Children[0])
					case "original":
						p.frames.Rows[0].Original = detachedOriginal{fieldSet{"Enabled": true}, true}
					case "error":
						row.Children[0].Value = "changed"
						return errors.New("root hook failed")
					case "panic":
						row.Children[0].Value = "changed"
						panic("root hook panic")
					case "cancel":
						cancel()
					}
					return nil
				}
				var err error
				panicked := false
				func() {
					defer func() { panicked = recover() != nil }()
					err = p.prepareFinitePhaseRoots(ctx, plan, probe)
				}()
				if (mode == "panic") != panicked || !panicked && err == nil || p.reconciliation.active || p.reconciliation.rootPrepared || p.executionFailure == nil {
					t.Fatal("failed root preparation retained usable authority", err, panicked)
				}
				if mode == "second-root-error" && (len(probe.rows) != 0 || !reflect.DeepEqual(hook.calls, []int{1, 2})) {
					t.Fatal("validation ran after failed root initialization")
				}
			})
		})
	}
}

func TestFiniteRootPreparationRepeatedRootOccurrences(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan, hook := rootPreparationFixture(t, ctx, func(p *Program, _ *phaseRootPreparationHook) {
			input := p.input.(*phaseOccurrenceInput)
			input.Rows[1] = input.Rows[0]
			p.frames.Rows[2].Entity = p.frames.Rows[0].Entity
			p.frames.Rows[2].Previous = p.frames.Rows[0].Previous
			p.frames.Rows[3].Entity = p.frames.Rows[1].Entity
			p.original = &OriginalInput{Presence: map[uintptr]originalPresence{}}
			for _, frame := range p.frames.Rows {
				frame.Original = p.captureEntityOriginal(frame.Record, frame.Entity)
			}
			if err := p.captureFiniteRootDecision(reflect.ValueOf(input.Rows)); err != nil {
				t.Fatal(err)
			}
		})
		if err := p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(hook.calls, []int{1, 1}) || !reflect.DeepEqual(hook.seenAssignments, []bool{false, false}) {
			t.Fatal("root occurrences or per-occurrence assignments collapsed", hook.calls, hook.seenAssignments)
		}
	})
}

func TestFiniteRootPreparationSwallowedFailureCannotCompleteEngine(t *testing.T) {
	for _, mode := range []string{"hook-error", "write", "allocate"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			var p *Program
			var hook *phaseRootPreparationHook
			_, err := engine.New().Execute(context.Background(), engine.Request{
				Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: db.DB},
				Handler: &phaseSelectionEngineHandler{test: t, prepare: func(program *Program) {
					p = program
					hook = configureRootPreparation(p, p.hook.Interface().(*phaseSelectionHook))
				}, execute: func(ctx context.Context, inv rhandler.Invocation, p *Program, c *finiteSourcePhases, _ *phaseSelectionHook) (any, error) {
					plan, e := p.selectFinitePhasePlan(ctx, c)
					if e != nil {
						return nil, e
					}
					dmlValue, _, e := inv.Binder.Lookup(ctx, h.DMLKey)
					if e != nil {
						return nil, e
					}
					allocator, _, e := inv.Binder.Lookup(ctx, h.SequencerKey)
					if e != nil {
						return nil, e
					}
					hook.run = func(ctx context.Context, row *phaseTestRoot, stage string) error {
						if stage != "Init" {
							return nil
						}
						switch mode {
						case "hook-error":
							return errors.New("root preparation sentinel")
						case "write":
							_ = dmlValue.(h.DML).Insert("no_root_writes", row)
						case "allocate":
							_ = allocator.(h.Sequencer).Allocate(ctx, "no_root_allocations", row, "ID")
						}
						return nil
					}
					_ = p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{})
					return nil, nil
				}},
			})
			if err == nil || p == nil || p.executionFailure == nil || p.reconciliation.rootPrepared {
				t.Fatal("swallowed root preparation failure completed", err)
			}
			if mode != "hook-error" && !errors.Is(p.executionFailure, engine.ErrWriteEligibilityMutation) {
				t.Fatal("native mutation veto not retained", p.executionFailure)
			}
		})
	}
}

func TestFiniteRootPreparationFrameworkValidationFailure(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan, _ := rootPreparationFixture(t, ctx)
		failure := errors.New("framework validation failed")
		if err := p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{failure: failure}); !errors.Is(err, failure) || p.reconciliation.rootPrepared || p.executionFailure == nil {
			t.Fatal("framework failure lost", err)
		}
	})
}

func TestFiniteRootPreparationRejectsPostSelectionChangesBeforeHooks(t *testing.T) {
	for _, mode := range []string{"root", "child", "current", "data", "status"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan, hook := rootPreparationFixture(t, ctx)
				switch mode {
				case "root":
					p.input.(*phaseOccurrenceInput).Rows[0].Enabled = true
				case "child":
					p.input.(*phaseOccurrenceInput).Rows[0].Children[0].Value = "changed"
				case "current":
					p.database.ByRecord[p.metadata.Root][0].Interface().(*phaseTestRoot).Enabled = true
				case "data":
					p.output.(*phaseSelectionOutput).Data = []*phaseTestRoot{{ID: 99}}
				case "status":
					p.output.(*phaseSelectionOutput).Status = "changed"
				}
				if err := p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); err == nil || len(hook.calls) != 0 || p.reconciliation.active || p.executionFailure == nil {
					t.Fatal("changed selection state reached root preparation", err, hook.calls)
				}
			})
		})
	}
}

func TestFiniteRootPreparationRequiresCanonicalUpdateCurrentBeforeHooks(t *testing.T) {
	for _, mode := range []string{"missing", "detached", "wrong-key", "canonical"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan, hook := rootPreparationFixture(t, ctx, func(p *Program, _ *phaseRootPreparationHook) {
					frame := p.frames.Rows[2]
					switch mode {
					case "missing":
						frame.Previous = reflect.Value{}
					case "detached":
						frame.Previous = reflect.ValueOf(&phaseTestRoot{ID: 2})
					case "wrong-key":
						frame.Previous.Interface().(*phaseTestRoot).ID = 99
					}
				})
				err := p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{})
				if mode == "canonical" {
					if err != nil || !p.reconciliation.rootPrepared {
						t.Fatal("canonical Current rejected", err)
					}
					return
				}
				if err == nil || len(hook.calls) != 0 || p.reconciliation.active || p.executionFailure == nil {
					t.Fatal("invalid Current reached root preparation", err, hook.calls)
				}
			})
		})
	}
}

func TestFiniteRootPreparationInsertPreservesOriginalIdentity(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, compiled, selection := phaseSelectionFixture(t)
		input := p.input.(*phaseOccurrenceInput)
		input.Rows[0].ID = 0
		input.Rows[0].Has.ID = false
		// One unsupplied root forces the original whole-request INSERT decision.
		p.original = &OriginalInput{Presence: map[uintptr]originalPresence{}}
		for _, frame := range p.frames.Rows {
			frame.Original = p.captureEntityOriginal(frame.Record, frame.Entity)
		}
		if err := p.captureFiniteRootDecision(reflect.ValueOf(input.Rows)); err != nil {
			t.Fatal(err)
		}
		compiled.insert = compiled.update
		hook := configureRootPreparation(p, selection)
		plan, err := p.selectFinitePhasePlan(ctx, compiled)
		if err != nil {
			t.Fatal(err)
		}
		if err = p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); err != nil {
			t.Fatal(err)
		}
		if !p.reconciliation.rootPrepared || !reflect.DeepEqual(hook.calls, []int{0, 2}) || input.Rows[0].ID != 0 || input.Rows[0].Has.ID || input.Rows[1].ID != 2 || !input.Rows[1].Has.ID {
			t.Fatal("INSERT preparation changed original identities", hook.calls)
		}
		for _, allocation := range p.reconciliation.allocations {
			if allocation.allocated.IsValid() {
				t.Fatal("INSERT preparation allocated")
			}
		}
	})
}
