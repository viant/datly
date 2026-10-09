package writer

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

func projectedActionFixture(t *testing.T, ctx context.Context, insert, repeated bool) (*Program, *finitePhasePlan, []*Action) {
	p, plan := rootProjectionFixture(t, ctx, insert, repeated, true)
	if err := p.projectFinitePhaseRoots(ctx, plan); err != nil {
		t.Fatal(err)
	}
	actions, err := p.mintFiniteRootActions(plan)
	if err != nil {
		t.Fatal(err)
	}
	return p, plan, actions
}
func TestProjectedRootActionCanonicalOwnership(t *testing.T) {
	for _, insert := range []bool{false, true} {
		for _, repeated := range []bool{false, true} {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan, actions := projectedActionFixture(t, ctx, insert, repeated)
				if len(actions) != len(p.reconciliation.roots) || p.actions != nil && len(p.actions.Rows) != 0 {
					t.Fatal("mint changed action journal")
				}
				for i, action := range actions {
					if p.actionFrame(action) != p.reconciliation.roots[i].frame || action.Entity.Pointer() == action.frame.Entity.Pointer() {
						t.Fatal("lost canonical detached association")
					}
					if action.Entity.Interface().(*phaseTestRoot).Enabled == insert {
						t.Fatal("wrong source image")
					}
				}
				p.actions = &MutationActions{Rows: append([]*Action(nil), actions...)}
				for _, action := range actions {
					if p.actionFrame(action) == nil {
						t.Fatal("action list publication invalidated payload ownership")
					}
				}
				if repeated && (actions[0] == actions[1] || actions[0].Entity.Pointer() != actions[1].Entity.Pointer()) {
					t.Fatal("duplicate occurrences deduplicated or aliases lost")
				}
				if _, err := p.mintFiniteRootActions(plan); err == nil || p.reconciliation.active || p.executionFailure == nil {
					t.Fatal("mint replay accepted")
				}
			})
		}
	}
}
func TestProjectedRootActionRejectsTampering(t *testing.T) {
	cases := map[string]func(*Program, *finitePhasePlan, []*Action){
		"removed-token": func(_ *Program, _ *finitePhasePlan, a []*Action) { a[0].projected = nil },
		"frame-kind":    func(_ *Program, _ *finitePhasePlan, a []*Action) { a[0].frame.Action = h.WriteDelete },
		"removed-token-live-row": func(_ *Program, _ *finitePhasePlan, a []*Action) {
			a[0].projected = nil
			a[0].Entity = a[0].frame.Entity
		},
		"foreign-program": func(p *Program, _ *finitePhasePlan, a []*Action) { copy := *p; a[0].projected.owner = &copy },
		"copy-action":     func(_ *Program, _ *finitePhasePlan, a []*Action) { copy := *a[0]; a[0] = &copy },
		"copy-token":      func(_ *Program, _ *finitePhasePlan, a []*Action) { copy := *a[0].projected; a[0].projected = &copy },
		"payload-pointer": func(_ *Program, _ *finitePhasePlan, a []*Action) {
			copy := *a[0].Entity.Interface().(*phaseTestRoot)
			a[0].Entity = reflect.ValueOf(&copy)
		},
		"payload-value": func(_ *Program, _ *finitePhasePlan, a []*Action) {
			a[0].Entity.Interface().(*phaseTestRoot).Enabled = true
		},
		"canonical-frame": func(_ *Program, _ *finitePhasePlan, a []*Action) { a[0].frame = a[1].frame },
		"kind":            func(_ *Program, _ *finitePhasePlan, a []*Action) { a[0].Kind = h.WriteDelete },
		"holder": func(p *Program, _ *finitePhasePlan, _ []*Action) {
			r := p.input.(*phaseOccurrenceInput).Rows[0]
			copy := *r
			p.input.(*phaseOccurrenceInput).Rows[0] = &copy
		},
		"retired":         func(p *Program, _ *finitePhasePlan, _ []*Action) { p.reconciliation.active = false },
		"plan":            func(_ *Program, plan *finitePhasePlan, _ []*Action) { plan.phases[0].phase.name = "changed" },
		"foreign-attempt": func(p *Program, _ *finitePhasePlan, _ []*Action) { copy := *p.reconciliation; p.reconciliation = &copy },
		"projection-pointer": func(p *Program, _ *finitePhasePlan, _ []*Action) {
			p.reconciliation.rootProjections[0].primary = p.reconciliation.rootProjections[1].primary
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan, a := projectedActionFixture(t, ctx, true, false)
				mutate(p, plan, a)
				if p.actionFrame(a[0]) != nil || p.executionFailure == nil || p.reconciliation.active {
					t.Fatal("tampered payload acquired frame")
				}
			})
		})
	}
}
func TestOrdinaryActionStillRequiresCanonicalPointer(t *testing.T) {
	f := newNativeGroupFixture(t)
	a := f.p.actions.Rows[0]
	copy := *a.Entity.Interface().(*qcRow)
	a.Entity = reflect.ValueOf(&copy)
	if f.p.actionFrame(a) != nil {
		t.Fatal("ordinary pointer rule weakened")
	}
}

// Native plumbing proof only: explicitly roll back. Source-mode completion and
// stage/admission progression remain closed and are not inferred from this test.
func TestProjectedRootPayloadNativeLoweringRollback(t *testing.T) {
	for _, insert := range []bool{false, true} {
		t.Run(map[bool]string{true: "insert", false: "update"}[insert], func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, _, actions := projectedActionFixture(t, ctx, insert, false)
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, `CREATE TABLE items(id INTEGER PRIMARY KEY,Enabled INTEGER)`); err != nil {
					t.Fatal(err)
				}
				initialCount := 0
				if !insert {
					if err := db.ExecStatements(ctx, `INSERT INTO items(id,Enabled) VALUES(1,0),(2,0)`); err != nil {
						t.Fatal(err)
					}
					initialCount = 2
				}
				data := dml.NewData(db.DB)
				if err := data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				if err := data.Start(ctx); err != nil {
					t.Fatal(err)
				}
				binder := &sqBinder{data: data}
				p.actions = &MutationActions{Rows: actions}
				if insert {
					p.metadata.Root.QueueContract = "source-slice"
					if err := p.mintSourceSliceGroup(actions); err != nil {
						t.Fatal(err)
					}
					if err := p.queueSourceSlice(ctx, binder, data, actions); err != nil {
						t.Fatal(err)
					}
				} else {
					for _, action := range actions {
						if err := p.queuePhysical(ctx, binder, data, action, p.actionFrame(action), nil); err != nil {
							t.Fatal(err)
						}
					}
				}
				var count int
				if err := db.DB.QueryRow("SELECT COUNT(*) FROM items").Scan(&count); err != nil || count != initialCount {
					t.Fatal("premature drain", count, err)
				}
				for _, root := range p.input.(*phaseOccurrenceInput).Rows {
					if !root.Enabled || root.Has.Enabled {
						t.Fatal("lowering changed working values or late presence")
					}
				}
				if err := data.Flush(ctx, "items"); err != nil {
					t.Fatal(err)
				}
				_, tx := data.InvocationTransaction()
				if tx == nil {
					t.Fatal("native flush lost transaction")
				}
				for _, action := range actions {
					row := action.Entity.Interface().(*phaseTestRoot)
					var enabled bool
					if err := tx.QueryRowContext(ctx, "SELECT Enabled FROM items WHERE id=?", row.ID).Scan(&enabled); err != nil || enabled != row.Enabled {
						t.Fatal("native SQL did not use projected payload", enabled, row.Enabled, err)
					}
				}
				if err := data.Complete(ctx, errors.New("explicit plumbing rollback")); err == nil {
					t.Fatal("rollback cause lost")
				}
				if err := db.DB.QueryRow("SELECT COUNT(*) FROM items").Scan(&count); err != nil || count != initialCount {
					t.Fatal("rollback left rows", count, err)
				}
				if !insert {
					var enabled int
					if err := db.DB.QueryRow("SELECT SUM(Enabled) FROM items").Scan(&enabled); err != nil || enabled != 0 {
						t.Fatal("rollback retained update", enabled, err)
					}
				}

			})
		})
	}
}
