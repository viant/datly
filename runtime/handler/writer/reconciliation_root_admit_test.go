package writer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
)

type finiteAdmissionSource struct{ native *dml.Data }

func (s finiteAdmissionSource) Open(context.Context) (h.Data, error) { return s.native, nil }

// Inspect actual native queued operations without executing them or adding a
// production introspection API. This witness is synchronous after admission.
func verifyNativeRootAdmissionQueue(t *testing.T, native *dml.Data, p *Program, prefix int, insert bool) {
	t.Helper()
	queue := reflect.ValueOf(native).Elem().FieldByName("queue")
	want := prefix
	if insert && prefix > 0 {
		want = 1
	}
	if queue.Len() != want {
		t.Fatal("native operation prefix mismatch", queue.Len(), want)
	}
	for i := 0; i < queue.Len(); i++ {
		op := queue.Index(i).Elem()
		kind := "update"
		if insert {
			kind = "insert"
		}
		if op.FieldByName("kind").String() != kind || op.FieldByName("table").String() != "items" || op.FieldByName("executed").Bool() {
			t.Fatal("native operation kind/order/drain changed")
		}
		payload := op.FieldByName("data").Elem()
		if insert {
			if op.FieldByName("queueContract").Uint() != uint64(rh.SourceSlice) || payload.Len() != prefix {
				t.Fatal("insert group split or contract lost")
			}
			for j := 0; j < payload.Len(); j++ {
				if payload.Index(j).Elem().FieldByName("ID").Int() != int64(p.input.(*phaseOccurrenceInput).Rows[j].ID) {
					t.Fatal("insert source order changed")
				}
			}
		} else if payload.Elem().FieldByName("ID").Int() != int64(p.input.(*phaseOccurrenceInput).Rows[i].ID) {
			t.Fatal("update source order changed")
		}
	}
}

func TestFiniteRootAdmissionActualEnginePrefixesAndRollback(t *testing.T) {
	for _, insert := range []bool{false, true} {
		for _, mode := range []string{"success", "hook-error", "hook-panic", "hook-mutation", "hook-swallowed", "cancel", "replay", "span", "queue-items", "container", "terminal"} {
			t.Run(map[bool]string{true: "insert", false: "update"}[insert]+"/"+mode, func(t *testing.T) {
				db := sqlite.New(t)
				if err := db.ExecStatements(context.Background(), "CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, Enabled INTEGER)"); err != nil {
					t.Fatal(err)
				}
				if !insert {
					if err := db.ExecStatements(context.Background(), "INSERT INTO items(id,Enabled) VALUES(1,0),(2,0)"); err != nil {
						t.Fatal(err)
					}
				}
				native := dml.NewData(db.DB)
				var p *Program
				var seen, appendPrefix int
				var success bool
				abort := errors.New("explicit root-stage abort")
				_, err := engine.New().Execute(context.Background(), engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: finiteAdmissionSource{native}, Handler: &phaseSelectionEngineHandler{test: t, prepare: func(program *Program) {
					p = program
					if insert {
						prepareAllocationEngineFixture(t, p, false)
					} else {
						configureRootPreparation(p, p.hook.Interface().(*phaseSelectionHook))
					}
					p.metadata.Root.QueueContract = "source-slice"
				}, execute: func(parent context.Context, inv rh.Invocation, p *Program, c *finiteSourcePhases, _ *phaseSelectionHook) (any, error) {
					ctx, cancel := context.WithCancel(parent)
					defer cancel()
					c.insert = c.update
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
					hook := &finiteQueueObservationHook{run: func(_ context.Context, row *phaseTestRoot) error {
						seen++
						switch mode {
						case "hook-error":
							return errors.New("first root hook failed")
						case "hook-panic":
							panic("first root hook panic")
						case "hook-swallowed":
							service, _, err := inv.Binder.Lookup(ctx, h.DMLKey)
							if err != nil {
								return err
							}
							if err = service.(h.DML).Insert("items", &phaseTestRoot{ID: 99}); err == nil {
								t.Fatal("hook admitted unrelated DML")
							}
						case "hook-mutation":
							row.Children[0].Value = "changed"
						case "cancel":
							cancel()
						}
						return nil
					}}
					for _, root := range p.reconciliation.roots {
						root.frame.Hook = reflect.ValueOf(hook)
					}
					var panicked any
					func() { defer func() { panicked = recover() }(); e = p.admitFinitePhaseRoots(ctx, plan) }()
					appendPrefix = p.reconciliation.rootAppendPrefix
					success = p.reconciliation.rootAdmitted
					verifyNativeRootAdmissionQueue(t, native, p, appendPrefix, insert)
					if strings.HasPrefix(mode, "hook-") || mode == "cancel" {
						want := 1
						if insert {
							want = 2
						}
						if appendPrefix != want || success || seen != 1 || p.reconciliation.active || p.executionFailure == nil || e == nil && panicked == nil {
							t.Fatal("bad root failure prefix", appendPrefix, success, seen, e, panicked)
						}
						return nil, p.executionFailure
					}
					if e != nil || panicked != nil || !success || appendPrefix != 2 || seen != 2 || p.executionGuardReady {
						t.Fatal("root stage failed", e, panicked, success, appendPrefix, seen)
					}
					for _, root := range p.reconciliation.roots {
						if root.frame.Entity.Interface().(*phaseTestRoot).Children[0].ID != 0 {
							t.Fatal("root stage allocated child")
						}
					}
					if mode == "replay" {
						if e = p.admitFinitePhaseRoots(ctx, plan); e == nil || p.reconciliation.active {
							t.Fatal("root replay accepted")
						}
						return nil, e
					}
					if mode == "span" {
						p.actions.Rows[1] = p.actions.Rows[0]
					}
					if mode == "queue-items" {
						p.queueItems = append(p.queueItems, p.actions.Rows[0])
					}
					if mode == "container" {
						p.actions = &MutationActions{Rows: p.actions.Rows}
					}
					if mode == "span" || mode == "queue-items" || mode == "container" {
						if e = native.ValidateExecutionGuards(ctx); e == nil || p.reconciliation.active {
							t.Fatal("changed span accepted")
						}
						return nil, e
					}
					if mode == "terminal" {
						return nil, nil
					} // Actual Engine must reject incomplete graph completion.
					var count int
					if e = db.DB.QueryRow("SELECT COUNT(*) FROM items").Scan(&count); e != nil {
						t.Fatal(e)
					}
					want := 2
					if insert {
						want = 0
					}
					if count != want {
						t.Fatal("root admission flushed", count)
					}
					return nil, abort
				}}})
				if err == nil {
					t.Fatal("incomplete stage committed")
				}
				if mode == "terminal" && !strings.Contains(err.Error(), "completion proof unavailable") {
					t.Fatal("terminal proof bypassed", err)
				}
				if mode == "success" && !errors.Is(err, abort) {
					t.Fatal("native flush failed before explicit abort", err)
				}
				var count, total int
				if e := db.DB.QueryRow("SELECT COUNT(*),COALESCE(SUM(Enabled),0) FROM items").Scan(&count, &total); e != nil {
					t.Fatal(e)
				}
				want := 2
				if insert {
					want = 0
				}
				if count != want || total != 0 {
					t.Fatal("rollback changed physical state", count, total)
				}
			})
		}
	}
}

type finiteAdmissionBinder struct {
	h.Binder
	value h.DML
}

func (b *finiteAdmissionBinder) Lookup(ctx context.Context, key h.ValueKey) (any, bool, error) {
	if key == h.DMLKey && b.value != nil {
		return b.value, true, nil
	}
	return b.Binder.Lookup(ctx, key)
}

func TestFiniteRootAdmissionEmptyAliasesAndPreflight(t *testing.T) {
	for _, mode := range []string{"empty", "repeated-insert", "repeated-update", "wrong-owner", "wrong-binder", "observer", "matched", "criteria", "contract", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			if e := db.ExecStatements(context.Background(), "CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT,Enabled INTEGER)"); e != nil {
				t.Fatal(e)
			}
			native := dml.NewData(db.DB)
			var p *Program
			abort := errors.New("explicit preflight abort")
			_, err := engine.New().Execute(context.Background(), engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: finiteAdmissionSource{native}, Handler: &phaseSelectionEngineHandler{test: t, prepare: func(program *Program) {
				p = program
				if mode == "empty" {
					p.input.(*phaseOccurrenceInput).Rows = nil
					p.frames.Rows = nil
					if e := p.captureFiniteRootDecision(reflect.ValueOf(p.input.(*phaseOccurrenceInput).Rows)); e != nil {
						t.Fatal(e)
					}
					configureRootPreparation(p, p.hook.Interface().(*phaseSelectionHook))
				} else if mode == "repeated-update" {
					rows := p.input.(*phaseOccurrenceInput).Rows
					rows[1] = rows[0]
					p.frames.Rows[2].Entity = p.frames.Rows[0].Entity
					p.frames.Rows[3].Entity = p.frames.Rows[1].Entity
					if e := p.captureFiniteRootDecision(reflect.ValueOf(rows)); e != nil {
						t.Fatal(e)
					}
					configureRootPreparation(p, p.hook.Interface().(*phaseSelectionHook))
					p.frames.Rows[2].Previous = p.frames.Rows[0].Previous
				} else {
					prepareAllocationEngineFixture(t, p, mode == "repeated-insert")
				}
				p.metadata.Root.QueueContract = "source-slice"
			}, execute: func(parent context.Context, inv rh.Invocation, p *Program, c *finiteSourcePhases, _ *phaseSelectionHook) (any, error) {
				ctx, cancel := context.WithCancel(parent)
				defer cancel()
				c.insert = c.update
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
				switch mode {
				case "wrong-owner":
					p.guardBinder = &finiteAdmissionBinder{Binder: p.guardBinder, value: dml.NewData(db.DB)}
				case "wrong-binder":
					p.guardBinder = &finiteAdmissionBinder{Binder: p.guardBinder}
				case "observer":
					p.hook = reflect.ValueOf(&queueRecorder{})
				case "matched":
					p.metadata.Root.ConcurrencyToken = &Field{Name: "Version"}
				case "criteria":
					g := 0
					p.metadata.Root.MutationPredicateGroup = &g
				case "contract":
					p.metadata.Root.QueueContract = "source-row"
				case "canceled":
					cancel()
				}
				e = p.admitFinitePhaseRoots(ctx, plan)
				if mode == "empty" || strings.HasPrefix(mode, "repeated-") {
					want := 2
					if mode == "empty" {
						want = 0
					}
					if e != nil || !p.reconciliation.rootAdmitted || p.reconciliation.rootAppendPrefix != want {
						t.Fatal("empty/alias root stage failed", e)
					}
					verifyNativeRootAdmissionQueue(t, native, p, want, mode != "repeated-update")
					if want == 2 && (p.actions.Rows[0] == p.actions.Rows[1] || p.actions.Rows[0].Entity.Pointer() != p.actions.Rows[1].Entity.Pointer()) {
						t.Fatal("alias occurrences deduplicated or detached aliases lost")
					}
					return nil, abort
				}
				if e == nil || p.reconciliation.active || p.reconciliation.rootAdmitted || p.reconciliation.rootAppendPrefix != 0 || len(p.actions.Rows) != 0 || reflect.ValueOf(native).Elem().FieldByName("queue").Len() != 0 {
					t.Fatal("preflight admitted roots", e)
				}
				return nil, e
			}}})
			if err == nil {
				t.Fatal("unfinished root stage completed")
			}
			if (mode == "empty" || strings.HasPrefix(mode, "repeated-")) && !errors.Is(err, abort) {
				t.Fatal("fixture failed before root boundary", err)
			}
		})
	}
}
