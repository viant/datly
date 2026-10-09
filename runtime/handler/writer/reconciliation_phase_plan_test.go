package writer

import (
	"context"
	"reflect"
	"testing"
)

func finitePhasePlanFixture(t *testing.T, ctx context.Context) (*Program, *finiteSourcePhases, ReconciliationPlan) {
	t.Helper()
	p, compiled, _ := phaseOccurrenceFixture(t)
	if err := p.prepareFinitePhaseOccurrences(ctx, compiled); err != nil {
		t.Fatal(err)
	}
	plan := ReconciliationPlan{}
	for _, root := range p.reconciliation.roots {
		plan.Roots = append(plan.Roots, ReconciliationRootPlan{Root: root.ref})
	}
	for _, phase := range compiled.update {
		entry := ReconciliationPhasePlan{Phase: phase.name}
		for _, root := range p.reconciliation.roots {
			selection := ReconciliationRootPhasePlan{Root: root.ref}
			if phase.name == "children" {
				selection.Selected = []ReconciliationSelection{{Occurrence: root.roles[0].working[0], Assignments: []ReconciliationAssignment{{Field: "Value", Value: "reached-later", MarkPresent: true}}}}
				selection.Deletes = append([]OccurrenceRef(nil), root.roles[0].current...)
				selection.Followup = []ReconciliationAssignment{{Field: "Enabled", Value: true}}
			}
			entry.Roots = append(entry.Roots, selection)
		}
		plan.Phases = append(plan.Phases, entry)
	}
	return p, compiled, plan
}

func TestFinitePhasePlanCopiesWithoutPublishingOrAllocating(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, compiled, plan := finitePhasePlanFixture(t, ctx)
		value := "captured"
		plan.Phases[1].Roots[0].Selected[0].Assignments = append(plan.Phases[1].Roots[0].Selected[0].Assignments, ReconciliationAssignment{Field: "Pointer", Value: &value})
		sealed, err := p.sealFinitePhasePlan(ctx, compiled, plan)
		if err != nil {
			t.Fatal(err)
		}
		if len(sealed.phases) != 2 || sealed.phases[1].phase.followup.placement != "after-group" || sealed.owner != p.reconciliation {
			t.Fatal("compiled schedule lost")
		}
		var ids []int
		for _, ref := range sealed.phases[1].current {
			ids = append(ids, ref.ticket.previous.Interface().(*phaseTestChild).ID)
		}
		if !reflect.DeepEqual(ids, []int{20, 10, 21, 11}) {
			t.Fatal("global Current order lost", ids)
		}
		plan.Phases[1].Roots[0].Selected[0].Assignments[0].Value = "changed"
		plan.Phases[1].Roots[0].Followup[0].Value = false
		plan.Phases[1].Roots[0].Deletes[0] = OccurrenceRef{}
		value = "mutated-pointer"
		if sealed.phases[1].roots[0].selected[0].Assignments[0].Value != "reached-later" || sealed.phases[1].roots[0].followup[0].Value != true || sealed.phases[1].roots[0].deletes[0].ticket == nil {
			t.Fatal("hook plan retained mutable authority")
		}
		for _, row := range p.input.(*phaseOccurrenceInput).Rows {
			if row.Enabled || row.Children[0].ID != 0 || row.Children[0].Value == "reached-later" {
				t.Fatal("business assignment or allocation published early")
			}
		}
		if captured := sealed.phases[1].roots[0].selected[0].Assignments[1].Value.(*string); *captured != "captured" || captured == &value {
			t.Fatal("pointer assignment retained hook storage")
		}
	})
}

func TestFinitePhasePlanRejectsAuthorityAndAssignmentsBeforePublication(t *testing.T) {
	cases := map[string]func(*Program, *finiteSourcePhases, *ReconciliationPlan){
		"missing-phase": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) { plan.Phases = plan.Phases[:1] },
		"phase-order": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[0], plan.Phases[1] = plan.Phases[1], plan.Phases[0]
		},
		"unknown-phase": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Phase = "sql-execute"
		},
		"missing-root": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) { plan.Roots = plan.Roots[:1] },
		"root-order": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Roots[0], plan.Roots[1] = plan.Roots[1], plan.Roots[0]
		},
		"phase-root-order": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			r := plan.Phases[1].Roots
			r[0], r[1] = r[1], r[0]
		},
		"legacy-roles": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Roots[0].Roles = []ReconciliationRolePlan{}
		},
		"foreign-parent": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Roots[0].Selected[0].Occurrence = plan.Phases[1].Roots[1].Selected[0].Occurrence
		},
		"forged-ticket": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Roots[0].Selected[0].Occurrence = OccurrenceRef{}
		},
		"Current-as-working": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Roots[0].Selected[0].Occurrence = plan.Phases[1].Roots[0].Deletes[0]
		},
		"working-as-delete": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Roots[0].Deletes[0] = plan.Phases[1].Roots[0].Selected[0].Occurrence
		},
		"repeat-working": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			r := &plan.Phases[1].Roots[0]
			r.Selected = append(r.Selected, r.Selected[0])
		},
		"repeat-delete": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			r := &plan.Phases[1].Roots[0]
			r.Deletes = append(r.Deletes, r.Deletes[0])
		},
		"identity-adoption": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Roots[0].Selected[0].AdoptCurrent = plan.Phases[1].Roots[0].Deletes[0]
		},
		"identity-assignment": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Roots[0].Selected[0].Assignments[0].Field = "ID"
		},
		"wrong-scalar-type": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Roots[0].Selected[0].Assignments[0].Value = 19
		},
		"later-root-type": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Roots[1].Selected[0].Assignments[0].Value = 19
		},
		"undeclared-deletes": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[0].Roots[0].Deletes = plan.Phases[1].Roots[0].Deletes
		},
		"duplicate-assignment": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			r := &plan.Phases[1].Roots[0].Selected[0]
			r.Assignments = append(r.Assignments, r.Assignments[0])
		},
		"unknown-followup": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[0].Roots[0].Followup = []ReconciliationAssignment{{Field: "Enabled", Value: true}}
		},
		"followup-identity": func(_ *Program, _ *finiteSourcePhases, plan *ReconciliationPlan) {
			plan.Phases[1].Roots[0].Followup[0].Field = "ID"
		},
		"retired":          func(p *Program, _ *finiteSourcePhases, _ *ReconciliationPlan) { p.reconciliation.active = false },
		"foreign-compiler": func(_ *Program, c *finiteSourcePhases, _ *ReconciliationPlan) { c.root = &Record{} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, c, plan := finitePhasePlanFixture(t, ctx)
				mutate(p, c, &plan)
				if sealed, err := p.sealFinitePhasePlan(ctx, c, plan); err == nil || sealed != nil {
					t.Fatal("invalid plan sealed")
				}
				for _, row := range p.input.(*phaseOccurrenceInput).Rows {
					if row.Enabled || row.Children[0].ID != 0 || row.Children[0].Value == "reached-later" {
						t.Fatal("failed plan published partial business data")
					}
				}
			})
		})
	}
}

func TestFinitePhasePlanDisjointInsertPhasesShareCanonicalHolder(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, compiled, input := phaseOccurrenceFixture(t)
		input.Rows[1].Has.ID = false // Original mixed request selects INSERT schedule.
		if err := p.captureFiniteRootDecision(reflect.ValueOf(input.Rows)); err != nil {
			t.Fatal(err)
		}
		input.Rows[0].Details = []*phaseTestChild{{Value: "a"}, {Value: "b"}}
		for i, row := range input.Rows[0].Details {
			p.frames.Rows = append(p.frames.Rows, &Frame{Record: p.metadata.Root.Relations[1].Child, Entity: reflect.ValueOf(row), Parent: p.frames.Rows[0], Original: detachedOriginal{fieldSet{"Value": true}, true}, holderIndexed: true, holderTracked: true, holderPosition: i})
		}
		if err := p.prepareFinitePhaseOccurrences(ctx, compiled); err != nil {
			t.Fatal(err)
		}
		plan := ReconciliationPlan{}
		for _, root := range p.reconciliation.roots {
			plan.Roots = append(plan.Roots, ReconciliationRootPlan{Root: root.ref})
		}
		for i, phase := range compiled.insert {
			entry := ReconciliationPhasePlan{Phase: phase.name}
			for j, root := range p.reconciliation.roots {
				selection := ReconciliationRootPhasePlan{Root: root.ref}
				if j == 0 && i < 2 {
					selection.Selected = []ReconciliationSelection{{Occurrence: root.roles[1].working[i]}}
				}
				entry.Roots = append(entry.Roots, selection)
			}
			plan.Phases = append(plan.Phases, entry)
		}
		sealed, err := p.sealFinitePhasePlan(ctx, compiled, plan)
		if err != nil || len(sealed.phases) != 3 || sealed.phases[0].phase.role != sealed.phases[1].phase.role || sealed.phases[2].phase.followup.placement != "after-phase" {
			t.Fatal("disjoint shared holder lost", err)
		}
		plan.Phases[1].Roots[0].Selected[0].Occurrence = plan.Phases[0].Roots[0].Selected[0].Occurrence
		if _, err = p.sealFinitePhasePlan(ctx, compiled, plan); err == nil {
			t.Fatal("same working occurrence consumed by two phases")
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err = p.sealFinitePhasePlan(cancelled, compiled, plan); err == nil {
			t.Fatal("cancelled plan sealed")
		}
	})
}

func TestFiniteLegacyPlanCannotIgnorePhaseSelections(t *testing.T) {
	p := &Program{}
	if err := p.applyReconciliationPlan(context.Background(), ReconciliationPlan{Phases: []ReconciliationPhasePlan{}}); err == nil {
		t.Fatal("legacy mode ignored source phase plan")
	}
}
