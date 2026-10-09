package writer

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type finiteRootAllocator struct {
	calls           int
	table, selector string
	rows            []*phaseTestRoot
	run             func(context.Context, []*phaseTestRoot) error
}

func (s *finiteRootAllocator) Allocate(ctx context.Context, table string, dest any, selector string) error {
	s.calls++
	s.table, s.selector = table, selector
	s.rows = dest.([]*phaseTestRoot)
	if s.run != nil {
		return s.run(ctx, s.rows)
	}
	next := 10
	for _, row := range s.rows {
		if row.ID == 0 {
			row.ID = next
			next++
		}
	}
	return nil
}

func allocationFixture(t *testing.T, ctx context.Context, setup ...func(*Program)) (*Program, *finitePhasePlan) {
	t.Helper()
	p, compiled, selection := phaseSelectionFixture(t)
	input := p.input.(*phaseOccurrenceInput)
	input.Rows[0].ID = 0
	input.Rows[0].Has.ID = false
	for _, change := range setup {
		change(p)
	}
	p.original = &OriginalInput{Presence: map[uintptr]originalPresence{}}
	for _, frame := range p.frames.Rows {
		frame.Original = p.captureEntityOriginal(frame.Record, frame.Entity)
	}
	if err := p.captureFiniteRootDecision(reflect.ValueOf(input.Rows)); err != nil {
		t.Fatal(err)
	}
	compiled.insert = compiled.update
	configureRootPreparation(p, selection)
	plan, err := p.selectFinitePhasePlan(ctx, compiled)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); err != nil {
		t.Fatal(err)
	}
	return p, plan
}

func TestFiniteRootAllocationUsesOrderedRootBatchAndSeparateEvidence(t *testing.T) {
	withPhaseOccurrenceContext(t, func(ctx context.Context) {
		p, plan := allocationFixture(t, ctx)
		original := p.finiteRootDecision.occurrences[0].key
		seq := &finiteRootAllocator{}
		if err := p.allocateFinitePhaseRoots(ctx, plan, seq); err != nil {
			t.Fatal(err)
		}
		input := p.input.(*phaseOccurrenceInput)
		if seq.calls != 1 || seq.table != "items" || seq.selector != "ID" || len(seq.rows) != 2 || seq.rows[0] != input.Rows[0] || seq.rows[1] != input.Rows[1] || input.Rows[0].ID != 10 || input.Rows[1].ID != 2 || input.Rows[0].Has.ID {
			t.Fatal("native batch or original identity/presence lost", seq, input.Rows)
		}
		if p.finiteRootDecision.occurrences[0].key != original || original != 0 || len(p.actions.Rows) != 0 {
			t.Fatal("allocation rewrote original or admitted actions")
		}
		if err := p.validateFiniteAllocatedRoots(plan); err != nil {
			t.Fatal(err)
		}
		if err := p.validateFiniteRootDecision(reflect.ValueOf(input.Rows)); err == nil {
			t.Fatal("original preallocation seal accepted new ID")
		}
		for _, root := range p.reconciliation.roots {
			image := p.reconciliation.allocations[root.frame].allocated.Interface().(*phaseTestRoot)
			if image == root.frame.Entity.Interface().(*phaseTestRoot) || image.ID != root.frame.Entity.Interface().(*phaseTestRoot).ID {
				t.Fatal("allocation image aliases working row")
			}
			for _, role := range root.roles {
				for _, child := range role.working {
					if p.reconciliation.allocations[child.ticket.frame].allocated.IsValid() || child.ticket.frame.Entity.Interface().(*phaseTestChild).ID != 0 {
						t.Fatal("unreached child allocated")
					}
				}
			}
		}
		input.Rows[0].ID++
		if err := p.validateFiniteAllocatedRoots(plan); err == nil {
			t.Fatal("arbitrary later identity accepted")
		}
	})
}

func TestFiniteRootAllocationUpdateAndEmptyDoNotDispatch(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "update", true: "empty"}[empty], func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				var p *Program
				var plan *finitePhasePlan
				if empty {
					p, plan = allocationFixture(t, ctx, func(p *Program) { p.input.(*phaseOccurrenceInput).Rows = nil; p.frames.Rows = nil })
				} else {
					var err error
					p, plan, _ = rootPreparationFixture(t, ctx)
					if err = p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); err != nil {
						t.Fatal(err)
					}
				}
				seq := &finiteRootAllocator{}
				if err := p.allocateFinitePhaseRoots(ctx, plan, seq); err != nil || seq.calls != 0 || !p.reconciliation.rootAllocated {
					t.Fatal("noninsert allocation", err, seq.calls)
				}
				if err := p.allocateFinitePhaseRoots(ctx, plan, seq); err == nil || p.reconciliation.active || p.reconciliation.rootAllocated || p.executionFailure == nil {
					t.Fatal("allocation replay retained authority", err)
				}
			})
		})
	}
}

func TestFiniteRootAllocationPreservesUnmarkedPositiveAndDuplicateOccurrences(t *testing.T) {
	for _, mode := range []string{"unmarked-positive", "repeated"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan := allocationFixture(t, ctx, func(p *Program) {
					input := p.input.(*phaseOccurrenceInput)
					if mode == "unmarked-positive" {
						input.Rows[0].ID = 7
						return
					}
					input.Rows[1] = input.Rows[0]
					p.frames.Rows[2].Entity = p.frames.Rows[0].Entity
					p.frames.Rows[3].Entity = p.frames.Rows[1].Entity
				})
				seq := &finiteRootAllocator{}
				if err := p.allocateFinitePhaseRoots(ctx, plan, seq); err != nil {
					t.Fatal(err)
				}
				if len(seq.rows) != 2 || seq.calls != 1 {
					t.Fatal("batch narrowed")
				}
				if mode == "unmarked-positive" {
					if seq.rows[0].ID != 7 || seq.rows[0].Has.ID {
						t.Fatal("unmarked identity reset")
					}
				} else if seq.rows[0] != seq.rows[1] || seq.rows[0].ID != 10 || !reflect.DeepEqual(p.reconciliation.allocatedRootKeys, []int64{10, 10}) {
					t.Fatal("duplicate source occurrences detached")
				}
			})
		})
	}
}

func TestFiniteRootAllocationRejectsPreparedStateChangesBeforeDispatch(t *testing.T) {
	for _, mode := range []string{"root", "child", "current", "output"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan := allocationFixture(t, ctx)
				switch mode {
				case "root":
					p.input.(*phaseOccurrenceInput).Rows[0].Enabled = false
				case "child":
					p.input.(*phaseOccurrenceInput).Rows[0].Children[0].Value = "changed"
				case "current":
					p.database.ByRecord[p.metadata.Root][0].Interface().(*phaseTestRoot).Enabled = true
				case "output":
					p.output.(*phaseSelectionOutput).Status = "changed"
				}
				seq := &finiteRootAllocator{}
				if err := p.allocateFinitePhaseRoots(ctx, plan, seq); err == nil || seq.calls != 0 || p.reconciliation.active || p.executionFailure == nil {
					t.Fatal("prepared mutation dispatched", err)
				}
			})
		})
	}
}

func TestFiniteRootAllocationFailureNeverPublishesEvidence(t *testing.T) {
	for _, mode := range []string{"child", "root-business", "marker", "current", "supplied-id", "partial-error", "panic", "cancel", "unresolved"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, plan := allocationFixture(t, ctx)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				seq := &finiteRootAllocator{run: func(_ context.Context, rows []*phaseTestRoot) error {
					if mode != "unresolved" {
						rows[0].ID = 10
					}
					switch mode {
					case "child":
						rows[0].Children[0].Value = "changed"
					case "root-business":
						rows[0].Enabled = false
					case "marker":
						rows[0].Has.ID = true
					case "current":
						p.database.ByRecord[p.metadata.Root][0].Interface().(*phaseTestRoot).Enabled = true
					case "supplied-id":
						rows[1].ID = 20
					case "partial-error":
						return errors.New("partial native allocation")
					case "panic":
						panic("native allocation panic")
					case "cancel":
						cancel()
					}
					return nil
				}}
				var err error
				panicked := false
				func() {
					defer func() { panicked = recover() != nil }()
					err = p.allocateFinitePhaseRoots(ctx, plan, seq)
				}()
				if (mode == "panic") != panicked || !panicked && err == nil || p.reconciliation.active || p.reconciliation.rootAllocated || p.executionFailure == nil {
					t.Fatal("allocation failure usable", err, panicked)
				}
				for _, allocation := range p.reconciliation.allocations {
					if allocation.allocated.IsValid() {
						t.Fatal("partial evidence published")
					}
				}
				if mode == "partial-error" && p.input.(*phaseOccurrenceInput).Rows[0].ID != 10 {
					t.Fatal("partial published ID reset")
				}
			})
		})
	}
}

var _ h.Sequencer = (*finiteRootAllocator)(nil)

func TestFiniteRootAllocationCaptureAllowsOnlySameRootStorage(t *testing.T) {
	for _, mode := range []string{"same-source-root", "distinct-root", "legacy-root", "cross-role"} {
		t.Run(mode, func(t *testing.T) {
			withPhaseOccurrenceContext(t, func(ctx context.Context) {
				p, _, input := phaseOccurrenceFixture(t)
				input.Rows[0].ID = 0
				input.Rows[1].ID = 0
				input.Rows[1].Children = input.Rows[0].Children
				p.frames.Rows[3].Entity = p.frames.Rows[1].Entity
				if mode != "distinct-root" {
					input.Rows[1] = input.Rows[0]
					p.frames.Rows[2].Entity = p.frames.Rows[0].Entity
				}
				if err := p.captureFiniteRootDecision(reflect.ValueOf(input.Rows)); err != nil {
					t.Fatal(err)
				}
				if mode == "legacy-root" {
					p.finiteRootDecision = nil
				}
				if mode == "cross-role" {
					p.frames.Rows[3].Record = p.metadata.Root.Relations[1].Child
				}
				err := p.captureReconciliationAllocation(ctx)
				if mode == "same-source-root" {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil {
					t.Fatal("unexplained storage association accepted")
				}
			})
		})
	}
}

func prepareAllocationEngineFixture(t *testing.T, p *Program, repeated bool) {
	input := p.input.(*phaseOccurrenceInput)
	input.Rows[0].ID = 0
	input.Rows[0].Has.ID = false
	if repeated {
		input.Rows[1] = input.Rows[0]
		p.frames.Rows[2].Entity = p.frames.Rows[0].Entity
		p.frames.Rows[3].Entity = p.frames.Rows[1].Entity
	}
	p.original = &OriginalInput{Presence: map[uintptr]originalPresence{}}
	for _, frame := range p.frames.Rows {
		frame.Original = p.captureEntityOriginal(frame.Record, frame.Entity)
	}
	if err := p.captureFiniteRootDecision(reflect.ValueOf(input.Rows)); err != nil {
		t.Fatal(err)
	}
	configureRootPreparation(p, p.hook.Interface().(*phaseSelectionHook))
}

func TestFiniteRootAllocationNativeAliasesConsumeRangeWithoutChildWork(t *testing.T) {
	db := sqlite.New(t)
	if err := db.ExecStatements(context.Background(), "CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, enabled INTEGER)"); err != nil {
		t.Fatal(err)
	}
	abort := errors.New("abort prepared root prefix")
	var p *Program
	_, err := engine.New().Execute(context.Background(), engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: db.DB}, Handler: &phaseSelectionEngineHandler{test: t, prepare: func(program *Program) { p = program; prepareAllocationEngineFixture(t, p, true) }, execute: func(ctx context.Context, inv rhandler.Invocation, p *Program, c *finiteSourcePhases, _ *phaseSelectionHook) (any, error) {
		c.insert = c.update
		plan, e := p.selectFinitePhasePlan(ctx, c)
		if e != nil {
			return nil, e
		}
		if e = p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); e != nil {
			return nil, e
		}
		capability, _, e := inv.Binder.Lookup(ctx, h.SequencerKey)
		if e != nil {
			return nil, e
		}
		if e = p.allocateFinitePhaseRoots(ctx, plan, capability.(h.Sequencer)); e != nil {
			return nil, e
		}
		input := p.input.(*phaseOccurrenceInput)
		if input.Rows[0] != input.Rows[1] || input.Rows[0].ID != 1 || input.Rows[0].Children[0].ID != 0 || input.Rows[0].Has.ID {
			t.Fatal("native alias or untouched child facts changed")
		}
		next := &phaseTestRoot{}
		if e = capability.(h.Sequencer).Allocate(ctx, "items", next, "ID"); e != nil {
			return nil, e
		}
		if next.ID != 3 {
			t.Fatalf("native batch did not reserve both original empty slots: next=%d", next.ID)
		}
		for _, allocation := range p.reconciliation.allocations {
			if allocation.frame.Parent != nil && allocation.allocated.IsValid() {
				t.Fatal("native root batch advanced children")
			}
		}
		return nil, abort
	}}})
	if !errors.Is(err, abort) {
		t.Fatal("native allocation failed before owned abort", err)
	}
	var count int
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM items").Scan(&count); err != nil || count != 0 {
		t.Fatal("root allocation persisted product rows", count, err)
	}
}

func TestFiniteRootAllocationSwallowedFailureCannotCompleteEngine(t *testing.T) {
	db := sqlite.New(t)
	var p *Program
	failure := errors.New("native root allocation failure")
	_, err := engine.New().Execute(context.Background(), engine.Request{Input: protectedEngineRoute(t, reflect.TypeFor[struct{}]()), DataSource: dml.Source{DB: db.DB}, Handler: &phaseSelectionEngineHandler{test: t, prepare: func(program *Program) { p = program; prepareAllocationEngineFixture(t, p, false) }, execute: func(ctx context.Context, inv rhandler.Invocation, p *Program, c *finiteSourcePhases, _ *phaseSelectionHook) (any, error) {
		c.insert = c.update
		plan, e := p.selectFinitePhasePlan(ctx, c)
		if e != nil {
			return nil, e
		}
		if e = p.prepareFinitePhaseRoots(ctx, plan, &subsetValidationProbe{}); e != nil {
			return nil, e
		}
		_ = p.allocateFinitePhaseRoots(ctx, plan, &finiteRootAllocator{run: func(context.Context, []*phaseTestRoot) error { return failure }})
		return nil, nil
	}}})
	if !errors.Is(err, failure) || p == nil || p.reconciliation.active || p.executionFailure == nil {
		t.Fatal("swallowed allocation failure completed", err)
	}
}
