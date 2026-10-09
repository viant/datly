package writer

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

func TestFinitePendingCurrentUpdateActualEngine(t *testing.T) {
	for _, mode := range []string{"success", "payload", "pointer", "markers", "candidate", "storage", "copy", "remove", "foreign", "cancel", "panic", "first-insert"} {
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
				p.hook.Interface().(*phaseSelectionHook).phases.update = p.hook.Interface().(*phaseSelectionHook).phases.update[1:]
				child := p.metadata.Root.Relations[0].Child
				child.Keys[0].Has = []int{4, 0}
				child.Sequence = &child.Keys[0]
				shared := "shared Current pointer"
				for i, row := range p.input.(*phaseOccurrenceInput).Rows {
					row.Children[0].ID = 10 + i*10
					row.Children[0].Has = &phaseTestHas{ID: true}
				}
				for _, row := range p.database.ByRecord[child] {
					row.Interface().(*phaseTestChild).Pointer = &shared
					row.Interface().(*phaseTestChild).Has = &phaseTestHas{ID: true}
				}

				if mode == "first-insert" {
					p.input.(*phaseOccurrenceInput).Rows[0].Children[0].ID = 0
				}
				p.original = &OriginalInput{Presence: map[uintptr]originalPresence{}}
				for _, frame := range p.frames.Rows {
					if frame.Parent != nil {
						frame.Original = p.captureEntityOriginal(frame.Record, frame.Entity)
					}
				}
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

				u, e := p.observeFiniteCurrentUpdate(ctx, ticket)
				if mode == "first-insert" {
					if e != nil || u != nil || p.finitePendingUpdate != nil || !p.reconciliation.active || p.finiteCursor.position != 0 {
						t.Fatal("first INSERT failed or successor classified", e)
					}
					after, e = p.finiteAllocationClassificationState()
					if e != nil || before != after {
						t.Fatal("INSERT observation changed state", e)
					}
					return nil, abort
				}

				if e != nil || u == nil {
					t.Fatal("no pending image", e)
				}
				current := u.current.ticket.previous.Interface().(*phaseTestChild)
				image := u.payload.Interface().(*phaseTestChild)
				candidate := u.candidate.Index(u.ordinal).Interface().(*phaseTestChild)
				if image.ID != 10 || image.ParentID != 1 || image.Value != "planned" || *image.Pointer != "captured" || current.Value == "planned" || p.input.(*phaseOccurrenceInput).Rows[0].Children[0].Value == "planned" {
					t.Fatal("Current/incoming projection conflated")
				}
				if image == candidate || image.Has != candidate.Has || image.Pointer != candidate.Pointer {
					t.Fatal("shallow source scalar capture lost")
				}
				base := u.storage
				if base.Index(0).Interface().(*phaseTestChild).Pointer != base.Index(1).Interface().(*phaseTestChild).Pointer || base.Index(0).Interface().(*phaseTestChild).Pointer == current.Pointer {
					t.Fatal("detached Current domain alias lost")
				}
				if after, e = p.finiteAllocationClassificationState(); e != nil || after != before {
					t.Fatal("projection advanced live Current or incoming", e)
				}
				checkctx := ctx
				switch mode {
				case "success":
					again, e := p.observeFiniteCurrentUpdate(ctx, ticket)
					if e != nil || again != u {
						t.Fatal("observation minted successor", e)
					}
					return nil, abort
				case "payload":
					image.Value = "tampered"
				case "pointer":
					replacement := "tampered"
					image.Pointer = &replacement
				case "markers":
					image.Has.ID = false
				case "candidate":
					candidate.Value = "tampered"
				case "storage":
					base.Index(0).Interface().(*phaseTestChild).Value = "tampered"
				case "copy":
					copy := *u
					p.finitePendingUpdate = &copy
					p.finiteCursor.pendingUpdate = &copy
				case "remove":
					p.finitePendingUpdate = nil
				case "foreign":
					ticket = &finitePhaseTransition{cursor: p.finiteCursor, predecessor: p.finiteCursor.checkpoint}
				case "cancel":
					var cancel context.CancelFunc
					checkctx, cancel = context.WithCancel(ctx)
					cancel()
				case "panic":
					p.metadata.OutputField = 99
					var v any
					func() { defer func() { v = recover() }(); _, _ = p.observeFiniteCurrentUpdate(ctx, ticket) }()
					if v == nil || p.reconciliation.active || p.executionFailure == nil {
						t.Fatal("pending panic not retired")
					}
					return nil, nil
				}
				_, e = p.observeFiniteCurrentUpdate(checkctx, ticket)
				if e == nil || p.reconciliation.active || p.executionFailure == nil {
					t.Fatal("changed candidate accepted", mode, e)
				}
				// Even a swallowed error must fail the actual owner completion.
				return nil, nil
			}}})
			if mode == "success" || mode == "first-insert" {
				if !errors.Is(err, abort) {
					t.Fatal(err)
				}
			} else if err == nil || program.executionFailure == nil {
				t.Fatal("unfinished or failed cursor completed", mode, err)
			}
			var count int
			if e := db.DB.QueryRow("SELECT COUNT(*) FROM items WHERE Enabled=0").Scan(&count); e != nil || count != 2 {
				t.Fatal("cursor drained or changed product rows", count, e)
			}
		})
	}
}
