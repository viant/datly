package writer

import (
	"context"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

func phaseActionFixture(t *testing.T, ctx context.Context, id int, marked, available bool, change func(*Program, *finiteSourcePhases)) (*Program, *finitePhasePlan) {
	t.Helper()
	p, c, in := phaseOccurrenceFixture(t)
	child := p.metadata.Root.Relations[0].Child
	child.Keys[0].Has = []int{4, 0}
	child.Sequence = &child.Keys[0]
	for _, root := range in.Rows {
		root.Children[0].ID = id
		if available {
			root.Children[0].Has = &phaseTestHas{ID: marked}
		}
	}
	if change != nil {
		change(p, c)
	}
	p.original = &OriginalInput{Presence: map[uintptr]originalPresence{}}
	for _, frame := range p.frames.Rows {
		frame.Original = p.captureEntityOriginal(frame.Record, frame.Entity)
	}
	if err := p.prepareFinitePhaseOccurrences(ctx, c); err != nil {
		t.Fatal(err)
	}
	plan := ReconciliationPlan{}
	for _, root := range p.reconciliation.roots {
		plan.Roots = append(plan.Roots, ReconciliationRootPlan{Root: root.ref})
	}
	for _, phase := range c.update {
		entry := ReconciliationPhasePlan{Phase: phase.name}
		for _, root := range p.reconciliation.roots {
			r := ReconciliationRootPhasePlan{Root: root.ref}
			if phase.name == "children" {
				r.Selected = []ReconciliationSelection{{Occurrence: root.roles[0].working[0]}}
			}
			entry.Roots = append(entry.Roots, r)
		}
		plan.Phases = append(plan.Phases, entry)
	}
	sealed, err := p.sealFinitePhasePlan(ctx, c, plan)
	if err != nil {
		t.Fatal(err)
	}
	return p, sealed
}
func TestFinitePhaseActionOriginalClassification(t *testing.T) {
	for _, tc := range []struct {
		name              string
		id                int
		marked, available bool
		want              h.WriteAction
		failure           bool
	}{
		{"marked-positive", 10, true, true, h.WriteUpdate, false},
		{"unmarked-positive", 10, false, true, h.WriteInsert, false},
		{"marked-zero", 0, true, true, h.WriteInsert, false},
		{"marked-negative", -2, true, true, h.WriteInsert, false},
		{"nil-marker", 10, false, false, h.WriteInsert, false},
		{"missing-positive", 99, true, true, h.WriteInsert, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan := phaseActionFixture(t, ctx, tc.id, tc.marked, tc.available, nil)
				// Avoid a second parent selecting an ID owned by the first parent.
				plan.phases[1].roots[1].selected = nil
				row := p.reconciliation.roots[0].frame.Entity.Interface().(*phaseTestRoot).Children[0]
				row.ID = 999
				row.Has = &phaseTestHas{ID: !tc.marked}
				row.Value = "live-value"
				plan.phases[1].roots[0].selected[0].Occurrence.ticket.frame.Action = h.WriteDelete
				result, err := p.classifyFinitePhasePlan(ctx, plan)
				if tc.failure {
					if err == nil || result != nil {
						t.Fatal("unmatched positive accepted")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				action := result.phases[1].roots[0][0]
				if action.kind != tc.want || row.ID != 999 || row.Value != "live-value" || row.Has.ID == tc.marked {
					t.Fatal("captured facts lost or data published")
				}
				if tc.want == h.WriteUpdate && action.current.ticket.previous.Interface().(*phaseTestChild).ID != 10 {
					t.Fatal("wrong Current")
				}
			})
		})
	}
}
func TestFinitePhaseActionInsertWorkflowAndParentKey(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := phaseActionFixture(t, ctx, 10, true, true, func(_ *Program, c *finiteSourcePhases) {
			c.update[1].workflow = "working-inserts"
			c.update[1].updateBasis = ""
		})
		p.reconciliation.roots[0].roles[0].working[0].ticket.frame.Previous = reflect.ValueOf(&phaseTestChild{ID: 10})
		result, err := p.classifyFinitePhasePlan(ctx, plan)
		if err != nil || result.phases[1].roots[0][0].kind != h.WriteInsert {
			t.Fatal("Previous reclassified INSERT", err)
		}
	})
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := phaseActionFixture(t, ctx, 0, false, true, func(p *Program, c *finiteSourcePhases) {
			child := p.metadata.Root.Relations[0].Child
			child.Keys = []Field{{Name: "ParentID", Index: []int{1}}}
			child.Sequence = nil
			c.update[1].updateBasis = "working"
			p.database.ByRecord[child] = []reflect.Value{reflect.ValueOf(&phaseTestChild{ID: 90, ParentID: 1}), reflect.ValueOf(&phaseTestChild{ID: 91, ParentID: 2})}
			for _, frame := range p.frames.Rows {
				if frame.Record == child {
					frame.Entity.Interface().(*phaseTestChild).ParentID = 0
				}
			}
		})
		result, err := p.classifyFinitePhasePlan(ctx, plan)
		if err != nil {
			t.Fatal(err)
		}
		for i, actions := range result.phases[1].roots {
			if actions[0].kind != h.WriteUpdate || actions[0].current.ticket.previous.Interface().(*phaseTestChild).ParentID != i+1 {
				t.Fatal("parent key substitution failed")
			}
		}
		if p.reconciliation.roots[0].roles[0].working[0].ticket.frame.Entity.Interface().(*phaseTestChild).ParentID != 0 {
			t.Fatal("parent link published")
		}
		p.finiteRootDecision.action = h.WriteInsert
		result, err = p.classifyFinitePhasePlan(ctx, plan)
		if err == nil || result != nil || !strings.Contains(err.Error(), "pending native parent") {
			t.Fatal("unresolved new parent treated as INSERT", err)
		}
	})
}
func TestFinitePhaseActionFailuresAndConflicts(t *testing.T) {
	for _, name := range []string{"missing-original", "foreign-parent", "unloaded-current", "ambiguous-current", "update-delete", "cross-wrapper-conflict", "cross-phase-conflict", "retired", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan := phaseActionFixture(t, ctx, 10, true, true, func(p *Program, c *finiteSourcePhases) {
					if name == "cross-wrapper-conflict" {
						in := p.input.(*phaseOccurrenceInput)
						in.Rows[1].ID = 1
						in.Rows[1].Children[0].ParentID = 1
						if err := p.captureFiniteRootDecision(reflect.ValueOf(in.Rows)); err != nil {
							t.Fatal(err)
						}
					}
					if name == "cross-phase-conflict" {
						c.update = append(c.update, finiteSourcePhase{name: "late-deletes", scope: "all-roots", workflow: "current-deletes", role: p.metadata.Root.Relations[0]})
					}
				})
				plan.phases[1].roots[1].selected = nil
				root := p.reconciliation.roots[0]
				child := p.metadata.Root.Relations[0].Child
				switch name {
				case "missing-original":
					root.roles[0].working[0].ticket.frame.Original = detachedOriginal{fieldSet{}, false}
				case "foreign-parent":
					root.roles[0].current[0].ticket.previous.Interface().(*phaseTestChild).ParentID = 2
				case "unloaded-current":
					delete(p.previousFields[child], "ParentID")
				case "ambiguous-current":
					ref := root.roles[0].current[0]
					copy := *ref.ticket
					copy.currentOrdinal = 99
					p.reconciliation.tickets[&copy] = true
					p.reconciliation.roots[0].roles[0].current = append(root.roles[0].current, OccurrenceRef{&copy})
				case "update-delete":
					plan.phases[1].roots[0].deletes = []OccurrenceRef{root.roles[0].current[0]}
				case "cross-wrapper-conflict":
					plan.phases[1].roots[1].deletes = []OccurrenceRef{p.reconciliation.roots[1].roles[0].current[0]}
				case "cross-phase-conflict":
					plan.phases[2].roots[0].deletes = []OccurrenceRef{root.roles[0].current[0]}
				case "retired":
					p.reconciliation.active = false
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					ctx = cancelled
				}
				result, err := p.classifyFinitePhasePlan(ctx, plan)
				if err == nil || result != nil {
					t.Fatal("invalid classification returned", name)
				}
				row := root.frame.Entity.Interface().(*phaseTestRoot).Children[0]
				if row.ID != 10 || row.Value != "new-a" {
					t.Fatal("failed classification published data")
				}
			})
		})
	}
}
func TestFinitePhaseActionRepeatedUpdatesRetainOccurrences(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := phaseActionFixture(t, ctx, 10, true, true, func(p *Program, _ *finiteSourcePhases) {
			root := p.input.(*phaseOccurrenceInput).Rows[0]
			root.Children = append(root.Children, &phaseTestChild{ID: 10, ParentID: 1, Has: &phaseTestHas{ID: true}})
			parent := p.frames.Rows[0]
			p.frames.Rows = append(p.frames.Rows, &Frame{Record: p.metadata.Root.Relations[0].Child, Parent: parent, Entity: reflect.ValueOf(root.Children[1]), holderPosition: 1, holderIndexed: true, holderTracked: true})
		})
		plan.phases[1].roots[1].selected = nil
		// Re-seal the actual two-slot graph rather than fabricating tickets.
		compiled := &finiteSourcePhases{root: p.metadata.Root, fields: map[string]Field{}, update: []finiteSourcePhase{*plan.phases[0].phase, *plan.phases[1].phase}}
		raw := ReconciliationPlan{}
		for _, root := range p.reconciliation.roots {
			raw.Roots = append(raw.Roots, ReconciliationRootPlan{Root: root.ref})
		}
		for _, phase := range compiled.update {
			entry := ReconciliationPhasePlan{Phase: phase.name}
			for i, root := range p.reconciliation.roots {
				r := ReconciliationRootPhasePlan{Root: root.ref}
				if phase.name == "children" && i == 0 {
					for _, ref := range root.roles[0].working {
						r.Selected = append(r.Selected, ReconciliationSelection{Occurrence: ref})
					}
				}
				entry.Roots = append(entry.Roots, r)
			}
			raw.Phases = append(raw.Phases, entry)
		}
		// The first sealing retains the canonical compiler; recover its authority.
		compiled.root = p.metadata.Root
		compiled.roles = nil
		for _, role := range p.metadata.Root.Relations {
			compiled.roles = append(compiled.roles, finiteSourceRole{relation: role, holder: p.metadata.Root.EntityType.FieldByIndex(role.Field).Name, field: role.Field})
		}
		for i := range compiled.update {
			compiled.update[i].role = p.metadata.Root.Relations[1-i]
		}
		sealed, err := p.sealFinitePhasePlan(ctx, compiled, raw)
		if err != nil {
			t.Fatal(err)
		}
		result, err := p.classifyFinitePhasePlan(ctx, sealed)
		if err != nil {
			t.Fatal(err)
		}
		actions := result.phases[1].roots[0]
		if len(actions) != 2 || actions[0].current != actions[1].current || actions[0].occurrence == actions[1].occurrence {
			t.Fatal("repeated update collapsed")
		}
	})
}

func TestFinitePhaseActionEqualParentKeysRetainCanonicalCurrentWrapper(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := phaseActionFixture(t, ctx, 10, true, true, func(p *Program, _ *finiteSourcePhases) {
			in := p.input.(*phaseOccurrenceInput)
			in.Rows[1].ID = 1
			in.Rows[1].Children[0].ParentID = 1
			if err := p.captureFiniteRootDecision(reflect.ValueOf(in.Rows)); err != nil {
				t.Fatal(err)
			}
		})
		result, err := p.classifyFinitePhasePlan(ctx, plan)
		if err != nil {
			t.Fatal(err)
		}
		for i, actions := range result.phases[1].roots {
			current := true
			action := actions[0]
			if action.kind != h.WriteUpdate {
				t.Fatal("shared parent key lost update")
			}
			if _, err := p.reconciliation.resolve(action.current, p.reconciliation.roots[i].frame, p.metadata.Root.Relations[0].Child, &current); err != nil {
				t.Fatal("foreign Current wrapper returned", err)
			}
		}
	})
}
