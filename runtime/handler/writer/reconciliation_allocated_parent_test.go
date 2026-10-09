package writer

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

func allocatedParentFixture(t *testing.T, ctx context.Context) (*Program, *finitePhasePlan) {
	t.Helper()
	return allocationFixture(t, ctx, func(p *Program) {
		// A generic parent-keyed detail upsert, whose original incoming parent key
		// is unresolved until the existing root allocator succeeds.
		child := p.metadata.Root.Relations[0].Child
		child.Keys = []Field{{Name: "ParentID", Index: []int{1}}}
		child.Sequence = nil
		p.hook.Interface().(*phaseSelectionHook).phases.update[1].updateBasis = "working"
		p.database.ByRecord[child] = nil
		p.input.(*phaseOccurrenceInput).Rows[0].Children[0].ParentID = 0
	})
}

func TestFinitePhaseAllocatedParentClassification(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := allocatedParentFixture(t, ctx)
		if _, err := p.classifyFinitePhasePlan(ctx, plan); err == nil || !strings.Contains(err.Error(), "pending native parent allocation") {
			t.Fatal("pending parent classified", err)
		}
		if err := p.allocateFinitePhaseRoots(ctx, plan, &finiteRootAllocator{}); err != nil {
			t.Fatal(err)
		}
		result, err := p.classifyFinitePhasePlan(ctx, plan)
		if err != nil {
			t.Fatal(err)
		}
		for i, root := range p.reconciliation.roots {
			action := result.phases[1].roots[i][0]
			row := root.frame.Entity.Interface().(*phaseTestRoot)
			key, ok := action.occurrence.ticket.record.loadedKey(reflect.ValueOf(&phaseTestChild{ParentID: row.ID}).Elem())
			if !ok || action.identity != key || action.kind != h.WriteInsert || action.current.ticket != nil {
				t.Fatal("allocated linked identity or Current authority lost", action)
			}
			if i == 0 && row.Children[0].ParentID != 0 {
				t.Fatal("classification linked unreached child")
			}
		}
		if p.finiteRootDecision.occurrences[0].key != 0 || len(p.actions.Rows) != 0 {
			t.Fatal("classification changed original or admitted writes")
		}
	})
}

func TestFinitePhaseAllocatedParentRejectsAuthorityTampering(t *testing.T) {
	for _, mode := range []string{"root-id", "root-marker", "child", "current", "output", "image", "keys", "plan", "coordinated", "retired", "foreign-occurrence"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan := allocatedParentFixture(t, ctx)
				if err := p.allocateFinitePhaseRoots(ctx, plan, &finiteRootAllocator{}); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "root-id":
					p.input.(*phaseOccurrenceInput).Rows[0].ID++
				case "root-marker":
					p.input.(*phaseOccurrenceInput).Rows[0].Has.ID = true
				case "child":
					p.input.(*phaseOccurrenceInput).Rows[0].Children[0].Value = "changed"
				case "current":
					p.database.ByRecord[p.metadata.Root][0].Interface().(*phaseTestRoot).Enabled = true
				case "output":
					p.output.(*phaseSelectionOutput).Status = "changed"
				case "image":
					p.reconciliation.allocations[p.reconciliation.roots[0].frame].allocated.Interface().(*phaseTestRoot).ID++
				case "keys":
					p.reconciliation.allocatedRootKeys[0]++
				case "coordinated":
					p.input.(*phaseOccurrenceInput).Rows[0].ID++
					p.reconciliation.allocatedRootKeys[0]++
					p.reconciliation.allocations[p.reconciliation.roots[0].frame].allocated.Interface().(*phaseTestRoot).ID++
				case "retired":
					p.reconciliation.active = false
				case "foreign-occurrence":
					foreign := *plan.phases[1].roots[0].selected[0].Occurrence.ticket
					plan.phases[1].roots[0].selected[0].Occurrence.ticket = &foreign
				case "plan":
					copy := *plan
					plan = &copy
				}
				if result, err := p.classifyFinitePhasePlan(ctx, plan); err == nil || result != nil {
					t.Fatal("tampered allocated parent classified", mode)
				}
				if p.reconciliation.active || p.executionFailure == nil {
					t.Fatal("postallocation failure was not retained")
				}
				if result, err := p.classifyFinitePhasePlan(ctx, plan); err == nil || result != nil {
					t.Fatal("retired attempt classified")
				}
			})
		})
	}
}

func TestFinitePhaseAllocatedParentCancellation(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := allocatedParentFixture(t, ctx)
		if err := p.allocateFinitePhaseRoots(ctx, plan, &finiteRootAllocator{}); err != nil {
			t.Fatal(err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if result, err := p.classifyFinitePhasePlan(canceled, plan); err == nil || result != nil || p.reconciliation.active || p.executionFailure == nil {
			t.Fatal("canceled classification remained usable", err)
		}
	})
}

func TestFinitePhaseAllocatedParentSwallowedFailureCannotCompleteEngine(t *testing.T) {
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
		p.input.(*phaseOccurrenceInput).Rows[0].Children[0].Value = "changed after allocation"
		_, _ = p.classifyFinitePhasePlan(ctx, plan)
		return nil, nil
	}}})
	if err == nil || program == nil || program.reconciliation.active || program.executionFailure == nil {
		t.Fatal("swallowed classification failure completed", err)
	}
}

func TestFinitePhaseAllocatedParentRetainsCanonicalCurrent(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := allocationFixture(t, ctx, func(p *Program) {
			child := p.metadata.Root.Relations[0].Child
			child.Keys = []Field{{Name: "ParentID", Index: []int{1}}}
			child.Sequence = nil
			p.hook.Interface().(*phaseSelectionHook).phases.update[1].updateBasis = "working"
			p.database.ByRecord[child] = []reflect.Value{reflect.ValueOf(&phaseTestChild{ID: 20, ParentID: 2})}
			p.input.(*phaseOccurrenceInput).Rows[0].Children[0].ParentID = 0
		})
		if err := p.allocateFinitePhaseRoots(ctx, plan, &finiteRootAllocator{}); err != nil {
			t.Fatal(err)
		}
		result, err := p.classifyFinitePhasePlan(ctx, plan)
		if err != nil {
			t.Fatal(err)
		}
		first, second := result.phases[1].roots[0][0], result.phases[1].roots[1][0]
		if first.kind != h.WriteInsert || first.current.ticket != nil || second.kind != h.WriteUpdate || second.current.ticket == nil || second.current.ticket.root != p.reconciliation.roots[1].frame || second.current.ticket.previous.Interface().(*phaseTestChild).ID != 20 {
			t.Fatal("canonical Current lost")
		}
	})
}

func TestFinitePhaseAllocatedParentCannotFallBackToPreallocation(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := allocationFixture(t, ctx, func(p *Program) { p.input.(*phaseOccurrenceInput).Rows[0].ID = 1 })
		if p.finiteRootDecision.action != h.WriteInsert {
			t.Fatal("expected all-nonzero unmarked INSERT")
		}
		if err := p.allocateFinitePhaseRoots(ctx, plan, &finiteRootAllocator{}); err != nil {
			t.Fatal(err)
		}
		p.reconciliation.rootAllocated = false
		copy := *plan
		if result, err := p.classifyFinitePhasePlan(ctx, &copy); err == nil || result != nil || p.reconciliation.active || p.executionFailure == nil {
			t.Fatal("lost allocation bit restored preallocation authority", err)
		}
	})
}
