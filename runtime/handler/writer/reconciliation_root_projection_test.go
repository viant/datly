package writer

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/sql/dml"
	"reflect"
	"testing"
)

func rootProjectionFixture(t *testing.T, ctx context.Context, insert, repeated, followup bool, shared ...bool) (*Program, *finitePhasePlan) {
	p, plan, _ := rootPreparationFixture(t, ctx, func(p *Program, probe *phaseRootPreparationHook) {
		rows := p.input.(*phaseOccurrenceInput).Rows
		if insert {
			rows[0].ID = 0
			rows[0].Has.ID = false
		}
		if len(shared) > 0 && shared[0] {
			rows[1].Has = rows[0].Has
		}
		if repeated {
			rows[1] = rows[0]
			p.frames.Rows[2].Entity = p.frames.Rows[0].Entity
			p.frames.Rows[3].Entity = p.frames.Rows[1].Entity
			// Current root remains a real canonical match for the shared UPDATE root.
			p.frames.Rows[2].Previous = p.frames.Rows[0].Previous
		}
		p.original = &OriginalInput{Presence: map[uintptr]originalPresence{}}
		for _, frame := range p.frames.Rows {
			frame.Original = p.captureEntityOriginal(frame.Record, frame.Entity)
		}
		if err := p.captureFiniteRootDecision(reflect.ValueOf(rows)); err != nil {
			t.Fatal(err)
		}
		for _, frame := range p.frames.Rows {
			if frame.Parent == nil {
				frame.Action = p.finiteRootDecision.action
			}
		}
		selection := p.hook.Interface().(*phaseSelectionHook)
		selection.phases.insert = selection.phases.update
		if insert {
			selection.phases.update[1].followup.placement = "after-phase"
		}
		field := selection.phases.fields["Enabled"]
		field.Has = []int{4, 1}
		selection.phases.fields["Enabled"] = field
		p.metadata.Root.Fields[1] = field
		selection.phases.update[1].followup.fields["Enabled"] = field
		original := selection.run
		selection.run = func(ctx context.Context, in *phaseOccurrenceInput, out *phaseSelectionOutput) error {
			if err := original(ctx, in, out); err != nil {
				return err
			}
			if followup {
				for i := range selection.plan.Phases[1].Roots {
					value := false
					if len(shared) > 2 && shared[2] && i == 1 {
						value = true
					}
					selection.plan.Phases[1].Roots[i].Followup = []ReconciliationAssignment{{Field: "Enabled", Value: value, MarkPresent: !(repeated && i == 0)}}
				}
			}
			if len(shared) > 1 && shared[1] {
				for i := range selection.plan.Phases[1].Roots {
					selection.plan.Phases[1].Roots[i].Selected = nil
				}
			}
			return nil
		}
		probe.run = func(_ context.Context, row *phaseTestRoot, stage string) error {
			if stage == "Validate" {
				row.Has.Enabled = false
			}
			return nil
		}
	})
	if err := p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); err != nil {
		t.Fatal(err)
	}
	if err := p.allocateFinitePhaseRoots(ctx, plan, &finiteRootAllocator{}); err != nil {
		t.Fatal(err)
	}
	return p, plan
}

func TestFiniteRootProjectionSourceValuesAndPresence(t *testing.T) {
	for _, insert := range []bool{false, true} {
		for _, repeated := range []bool{false, true} {
			for _, followup := range []bool{false, true} {
				t.Run(map[bool]string{true: "insert", false: "update"}[insert]+map[bool]string{true: "-alias", false: "-distinct"}[repeated]+map[bool]string{true: "-followup", false: "-no-followup"}[followup], func(t *testing.T) {
					withPhaseOccurrenceContext(t, func(ctx context.Context) {
						p, plan := rootProjectionFixture(t, ctx, insert, repeated, followup)
						rows := p.input.(*phaseOccurrenceInput).Rows
						// Generic source reference: INSERT retains row storage; UPDATE shallow
						// copies scalar fields while keeping the live presence holder.
						sourceGraph, e := detachedRow(reflect.ValueOf(rows))
						if e != nil {
							t.Fatal(e)
						}
						source := sourceGraph.([]*phaseTestRoot)
						primary := make([]*phaseTestRoot, len(source))
						for i, row := range source {
							if insert {
								primary[i] = row
							} else {
								copy := *row
								primary[i] = &copy
							}
						}
						if followup {
							for _, row := range source {
								row.Enabled = false
								row.Has.Enabled = true
							}
						}
						if err := p.projectFinitePhaseRoots(ctx, plan); err != nil {
							t.Fatal(err)
						}
						for i, image := range p.reconciliation.rootProjections {
							got := image.primary.Interface().(*phaseTestRoot)
							if got.ID != primary[i].ID || got.Enabled != primary[i].Enabled || got.Has.Enabled != primary[i].Has.Enabled || got.Has.ID != primary[i].Has.ID {
								t.Fatal("source mapped payload/presence mismatch")
							}
							if image.root != p.reconciliation.roots[i].ref || image.action != p.finiteRootDecision.action {
								t.Fatal("projection occurrence authority lost")
							}
							if followup {
								if !image.followup.IsValid() || image.followup.Interface().(*phaseTestRoot).Enabled || !image.followup.Interface().(*phaseTestRoot).Has.Enabled || image.placement != map[bool]string{true: "after-phase", false: "after-group"}[insert] {
									t.Fatal("followup image mismatch")
								}
							}
							if rows[i].Enabled != true || rows[i].Has.Enabled || p.reconciliation.allocations[p.reconciliation.roots[i].frame].allocated.Interface().(*phaseTestRoot).Enabled != true {
								t.Fatal("future assignment published early")
							}
						}
						if insert && repeated && p.reconciliation.rootProjections[0].primary.Pointer() != p.reconciliation.rootProjections[1].primary.Pointer() {
							t.Fatal("whole batch clone lost duplicate storage")
						}
						if repeated && p.reconciliation.rootProjections[0].primary.Interface().(*phaseTestRoot).Has != p.reconciliation.rootProjections[1].primary.Interface().(*phaseTestRoot).Has {
							t.Fatal("shared presence alias lost")
						}
						if err := p.validateFiniteRootProjections(plan); err != nil {
							t.Fatal(err)
						}
						if len(p.actions.Rows) != 0 || len(p.queueItems) != 0 {
							t.Fatal("projection admitted actions")
						}
						if err := p.projectFinitePhaseRoots(ctx, plan); err == nil || p.reconciliation.active || p.executionFailure == nil || p.reconciliation.rootProjections != nil {
							t.Fatal("projection replay retained usable authority")
						}
					})
				})
			}
		}
	}
}

func TestFiniteRootProjectionRejectsChangedPlanAndState(t *testing.T) {
	for _, mode := range []string{"assignment", "current-order", "relation", "working", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan := rootProjectionFixture(t, ctx, true, false, true)
				switch mode {
				case "assignment":
					plan.phases[1].roots[0].followup[0].Value = true
				case "current-order":
					plan.phases[1].current = append(plan.phases[1].current, plan.phases[1].roots[0].root)
				case "relation":
					plan.phases[1].phase.role.Field = []int{3}
				case "working":
					p.input.(*phaseOccurrenceInput).Rows[0].Enabled = false
				case "canceled":
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					ctx = canceled
				}
				if err := p.projectFinitePhaseRoots(ctx, plan); err == nil || p.reconciliation.active || p.executionFailure == nil || len(p.reconciliation.rootProjections) != 0 || p.reconciliation.projectionState != "" {
					t.Fatal("invalid projection published", err)
				}
			})
		})
	}
}

// Pointer replacement preserves an earlier source shallow scalar, whereas
// direct pointee mutation would affect it. Native declared assignments replace.
func TestFiniteRootProjectionPointerReplacementOracle(t *testing.T) {
	type has struct{ Value bool }
	type row struct {
		ID    int
		Value *bool
		Has   *has
	}
	old, newValue := true, false
	source := &row{ID: 1, Value: &old, Has: &has{}}
	early := *source
	futureAny, err := detachedRow(reflect.ValueOf([]*row{source, source}))
	if err != nil {
		t.Fatal(err)
	}
	future := futureAny.([]*row)
	record := &Record{EntityType: reflect.TypeFor[row](), Fields: []Field{{Name: "Value", Index: []int{1}, Has: []int{2, 0}}}}
	field := record.Fields[0]
	p := &Program{metadata: &Metadata{Root: record}}
	if err = p.applyReconciliationAssignments(reflect.ValueOf(future[0]), &Frame{Record: record}, map[string]Field{"Value": field}, []ReconciliationAssignment{{Field: "Value", Value: &newValue, MarkPresent: true}}, nil); err != nil {
		t.Fatal(err)
	}
	nativeEarlyAny, err := detachedRow(reflect.ValueOf(&early))
	if err != nil {
		t.Fatal(err)
	}
	nativeEarly := reflect.ValueOf(nativeEarlyAny)
	if err = p.copyFiniteProjectionMarkers(nativeEarly, reflect.ValueOf(future[0])); err != nil {
		t.Fatal(err)
	}
	source.Value = &newValue
	source.Has.Value = true
	got := nativeEarly.Interface().(*row)
	if *got.Value != *early.Value || got.Has.Value != early.Has.Value || future[0] != future[1] || *future[1].Value != false || *source.Value != false {
		t.Fatal("pointer replacement or alias semantics changed")
	}
	// Separate source control distinguishes pointee mutation from replacement.
	pointee := true
	source.Value = &pointee
	early = *source
	pointee = false
	if *early.Value {
		t.Fatal("source pointee mutation control failed")
	}
}

func TestFiniteRootProjectionSharedMarkersAcrossDistinctRoots(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := rootProjectionFixture(t, ctx, false, false, true, true)
		if err := p.projectFinitePhaseRoots(ctx, plan); err != nil {
			t.Fatal(err)
		}
		first, second := p.reconciliation.rootProjections[0].primary.Interface().(*phaseTestRoot), p.reconciliation.rootProjections[1].primary.Interface().(*phaseTestRoot)
		if first == second || first.Has != second.Has || !first.Has.Enabled {
			t.Fatal("distinct roots lost source shared marker storage")
		}
		p.reconciliation.rootProjections[0].followup.Interface().(*phaseTestRoot).Enabled = true
		if err := p.validateFiniteRootProjections(plan); err == nil || p.reconciliation.active || p.executionFailure == nil {
			t.Fatal("sealed image mutation accepted")
		}
	})
}

func TestFiniteRootProjectionEmptyGroupsHaveNoImplicitFollowup(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := rootProjectionFixture(t, ctx, true, false, false, false, true)
		for _, root := range plan.phases[1].roots {
			if len(root.selected) != 0 {
				t.Fatal("expected empty selected groups")
			}
		}
		if err := p.projectFinitePhaseRoots(ctx, plan); err != nil {
			t.Fatal(err)
		}
		for _, image := range p.reconciliation.rootProjections {
			if image.followup.IsValid() || !image.primary.Interface().(*phaseTestRoot).Enabled || image.primary.Interface().(*phaseTestRoot).Has.Enabled {
				t.Fatal("empty group invented followup")
			}
		}
	})
}

type rootProjectionPanicContext struct{ context.Context }

func (rootProjectionPanicContext) Err() error { panic("projection context panic") }
func TestFiniteRootProjectionPanicRetiresAuthority(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := rootProjectionFixture(t, ctx, true, false, true)
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("panic did not propagate")
				}
			}()
			_ = p.projectFinitePhaseRoots(rootProjectionPanicContext{ctx}, plan)
		}()
		if p.reconciliation.active || p.executionFailure == nil || len(p.reconciliation.rootProjections) != 0 || p.reconciliation.projectionState != "" {
			t.Fatal("panicked projection retained usable authority")
		}
	})
}

func TestFiniteRootProjectionSwallowedFailureCannotCompleteEngine(t *testing.T) {
	db := sqlite.New(t)
	var program *Program
	_, err := engine.New().Execute(context.Background(), engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: db.DB}, Handler: &phaseSelectionEngineHandler{test: t, prepare: func(p *Program) { program = p; prepareAllocationEngineFixture(t, p, false) }, execute: func(ctx context.Context, inv rhandler.Invocation, p *Program, c *finiteSourcePhases, _ *phaseSelectionHook) (any, error) {
		c.insert = c.update
		plan, e := p.selectFinitePhasePlan(ctx, c)
		if e != nil {
			return nil, e
		}
		if e = p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); e != nil {
			return nil, e
		}
		if e = p.allocateFinitePhaseRoots(ctx, plan, &finiteRootAllocator{}); e != nil {
			return nil, e
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_ = p.projectFinitePhaseRoots(canceled, plan)
		return nil, nil
	}}})
	if !errors.Is(err, context.Canceled) || program == nil || program.reconciliation.active || program.executionFailure == nil {
		t.Fatal("swallowed projection failure completed", err)
	}
}

func TestFiniteRootProjectionPlacementPreservesRepeatedSourceSnapshots(t *testing.T) {
	for _, insert := range []bool{false, true} {
		t.Run(map[bool]string{true: "after-phase", false: "after-group"}[insert], func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan := rootProjectionFixture(t, ctx, insert, true, true, false, false, true)
				if err := p.projectFinitePhaseRoots(ctx, plan); err != nil {
					t.Fatal(err)
				}
				first, second := p.reconciliation.rootProjections[0].followup.Interface().(*phaseTestRoot), p.reconciliation.rootProjections[1].followup.Interface().(*phaseTestRoot)
				// Source shallow UPDATE at each group retains false then true; source
				// delayed updates after all groups both capture the final shared true.
				if first.Enabled != insert || !second.Enabled || first.Has != second.Has || !first.Has.Enabled {
					t.Fatal("placement changed source scalar/presence snapshots", first, second)
				}
				if p.input.(*phaseOccurrenceInput).Rows[0].Has.Enabled {
					t.Fatal("late presence published to working request")
				}
			})
		})
	}
}

func TestFiniteRootProjectionValidationPanicRetiresAuthority(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := rootProjectionFixture(t, ctx, true, false, true)
		if err := p.projectFinitePhaseRoots(ctx, plan); err != nil {
			t.Fatal(err)
		}
		inputField := p.metadata.InputField
		p.metadata.InputField = -1
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("validation panic did not propagate")
				}
			}()
			_ = p.validateFiniteRootProjections(plan)
		}()
		p.metadata.InputField = inputField
		if p.reconciliation.active || p.executionFailure == nil || len(p.reconciliation.rootProjections) != 0 || p.reconciliation.projectionState != "" {
			t.Fatal("validation panic retained usable projection")
		}
	})
}
