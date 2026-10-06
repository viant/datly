package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

func auxiliaryNullComponent(root bool) *spec.Component {
	c := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
	if root {
		c.RootView.Auxiliary = true
		c.RootView.RootNullPolicy = "skip-auxiliary"
	} else {
		c.RootView.Relations[0].View.Auxiliary = true
		c.RootView.Relations[0].View.NestedNullPolicy = "skip-auxiliary"
	}
	return c
}

func TestAuxiliaryNullCaptureFramesAndPositions(t *testing.T) {
	for _, root := range []bool{true, false} {
		for _, slots := range [][]int{{0}, {1}, {2}, {0, 2}} {
			t.Run(fmt.Sprint(root, slots), func(t *testing.T) {
				handler, err := New(auxiliaryNullComponent(root), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
				if err != nil {
					t.Fatal(err)
				}
				in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true, Children: true}}, {ID: ptr(2), Has: &sqPlainParentHas{ID: true, Children: true}}, {ID: ptr(3), Has: &sqPlainParentHas{ID: true, Children: true}}}}
				for _, row := range in.Rows {
					row.Children = []*sqPlainChild{{ID: ptr(11), Has: &sqPlainChildHas{ID: true}}, {ID: ptr(12), Has: &sqPlainChildHas{ID: true}}, {ID: ptr(13), Has: &sqPlainChildHas{ID: true}}}
				}
				if root {
					for _, i := range slots {
						in.Rows[i] = nil
					}
				} else {
					for _, row := range in.Rows {
						for _, i := range slots {
							row.Children[i] = nil
						}
					}
				}
				snapshot, err := handler.CaptureInput(context.Background(), in)
				if err != nil {
					t.Fatal(err)
				}
				program := snapshot.(*Program)
				if err = program.buildRecordFrames(context.Background(), nil, program.metadata.Root, reflect.ValueOf(in.Rows), nil); err != nil {
					t.Fatal(err)
				}
				want := 12 - 4*len(slots)
				if !root {
					want = 12 - 3*len(slots)
				}
				if len(program.frames.Rows) != want || len(program.original.Presence) != want {
					t.Fatalf("frames=%d presence=%d want=%d", len(program.frames.Rows), len(program.original.Presence), want)
				}
				for _, frame := range program.frames.Rows {
					if frame.Entity.IsNil() {
						t.Fatal("nil entity frame")
					}
				}
				if root {
					for _, i := range slots {
						if in.Rows[i] != nil {
							t.Fatal("root compacted")
						}
					}
				} else {
					for _, row := range in.Rows {
						if !row.Has.Children || len(row.Children) != 3 {
							t.Fatal("parent presence/length altered")
						}
						for _, i := range slots {
							if row.Children[i] != nil {
								t.Fatal("nested compacted")
							}
						}
					}
				}
			})
		}
	}
}

func TestAuxiliaryNullDoesNotPermitWritableDescendantNull(t *testing.T) {
	handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	in := &sqPlainInput{Rows: []*sqPlainParent{nil, {ID: ptr(1), Children: []*sqPlainChild{nil}}}}
	if _, err = handler.CaptureInput(context.Background(), in); err == nil {
		t.Fatal("policy inherited by writable child")
	}
}

func TestAuxiliaryNullInitClearedParentSkipsFramedDescendants(t *testing.T) {
	handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.HookType = reflect.TypeFor[nullIntroducingProbe]()
	in := queueInput(false)
	probe := handler.NewPhaseObserver().(*nullIntroducingProbe)
	probe.input = in
	ctx, _ := rhandler.WithPhaseObserver(context.Background(), probe, 1, 0)
	snapshot, err := handler.CaptureInput(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &finalValidationCapabilities{}})
	if err != nil {
		t.Fatal(err)
	}
	if probe.inits != 1 || in.Rows[1] != nil {
		t.Fatalf("inits=%d root=%v", probe.inits, in.Rows[1])
	}
	for _, frame := range snapshot.(*Program).frames.Rows {
		if strings.HasPrefix(frame.Location, "Rows[1]") {
			t.Fatalf("stale descendant survived: %s", frame.Location)
		}
	}
}

func TestAuxiliaryNullSQLiteAllocatesOnlyValidWritableChildren(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')")...); err != nil {
		t.Fatal(err)
	}
	child := &sqPlainChild{Label: ptr("inserted"), Has: &sqPlainChildHas{Label: true}}
	row := &sqPlainParent{ID: ptr(1), Name: ptr("must not write"), Children: []*sqPlainChild{child}, Has: &sqPlainParentHas{ID: true, Name: true, Children: true}}
	in := &sqPlainInput{Rows: []*sqPlainParent{nil, row, nil}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
	result, err := runSQLiteWriter(t, ctx, db, auxiliaryNullComponent(true), in, &sqPlainOutput{}, "patch")
	if err != nil {
		t.Fatal(err)
	}
	out := result.(*sqPlainOutput)
	if len(out.Data) != 3 || out.Data[0] != nil || out.Data[1] != row || out.Data[2] != nil {
		t.Fatal("output positions or pointers changed")
	}
	parents := sqParents(t, ctx, db)
	children := sqChildren(t, ctx, db)
	if len(parents) != 1 || *parents[0].Label != "original" || len(children) != 1 || child.ID == nil || *child.ID == 0 || children[0].ID != *child.ID {
		t.Fatalf("parents=%v children=%v id=%v", parents, children, child.ID)
	}
	if child.ParentID == nil || *child.ParentID != 1 || children[0].ParentID == nil || *children[0].ParentID != 1 {
		t.Fatal("valid parent link lost")
	}
}

func TestAuxiliaryNullExactRuntimeAdmission(t *testing.T) {
	for _, root := range []bool{true, false} {
		for _, aux := range []bool{true, false} {
			c := auxiliaryNullComponent(root)
			target := c.RootView
			if !root {
				target = c.RootView.Relations[0].View
			}
			target.Auxiliary = aux
			_, err := New(c, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if (err == nil) != aux {
				t.Fatalf("root=%v aux=%v err=%v", root, aux, err)
			}
		}
	}
}

func TestAuxiliaryNullCaptureReplayAndLivePhaseBoundary(t *testing.T) {
	handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	in := queueInput(false)
	snapshot, err := handler.CaptureInput(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	program := snapshot.(*Program)
	if err = program.buildRecordFrames(context.Background(), nil, program.metadata.Root, reflect.ValueOf(in.Rows), nil); err != nil {
		t.Fatal(err)
	}
	var child *Frame
	for _, frame := range program.frames.Rows {
		if frame.Location == "Rows[1].Children[0]" {
			child = frame
		}
	}
	if child == nil {
		t.Fatal("missing genuine descendant frame")
	}
	in.Rows[1] = nil
	if err = program.callEntityHook(context.Background(), "Init", child); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"Validate", "AfterSequence", "AfterQueue"} {
		if err = program.callEntityHook(context.Background(), phase, child); err == nil || !strings.Contains(err.Error(), "Rows[1]") {
			t.Fatalf("late %s accepted stale child: %v", phase, err)
		}
	}
	// A fresh capture is independently derived from the current body. Removed
	// entities retain no original presence in a subsequent invocation snapshot.
	replay, err := handler.CaptureInput(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	again := replay.(*Program)
	if err = again.buildRecordFrames(context.Background(), nil, again.metadata.Root, reflect.ValueOf(in.Rows), nil); err != nil {
		t.Fatal(err)
	}
	if len(again.frames.Rows) != 2 || len(again.original.Presence) != 2 || len(program.original.Presence) != 4 {
		t.Fatal("capture state shared or removed subtree retained")
	}
	in.Rows[0].Has.Name = false
	if !program.original.Presence[reflect.ValueOf(in.Rows[0]).Pointer()].Has("Name") {
		t.Fatal("original marker changed with live marker")
	}
}

func TestAuxiliaryNullSQLiteLateFailureAndCallerRollback(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, cancelled := range []bool{false, true} {
			t.Run(fmt.Sprint("external=", external, "/cancel=", cancelled), func(t *testing.T) {
				base := context.Background()
				db := sqlite.New(t)
				if err := db.ExecStatements(base, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')", "CREATE TABLE caller_audit(id INTEGER PRIMARY KEY)")...); err != nil {
					t.Fatal(err)
				}
				var tx *sql.Tx
				var opts []sqldml.Option
				if external {
					var err error
					tx, err = db.DB.BeginTx(base, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					opts = append(opts, sqldml.WithTx(tx))
					if _, err = tx.ExecContext(base, "INSERT INTO caller_audit VALUES(1)"); err != nil {
						t.Fatal(err)
					}
				}
				data := sqldml.NewData(db.DB, opts...)
				if err := data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
				if err != nil {
					t.Fatal(err)
				}
				in := &sqPlainInput{Rows: []*sqPlainParent{nil, {ID: ptr(1), Children: []*sqPlainChild{{ID: ptr(11), Label: ptr("rolled back"), Has: &sqPlainChildHas{ID: true, Label: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}}, nil}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
				snapshot, err := handler.CaptureInput(base, in)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = handler.Execute(base, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &sqBinder{data: data}}); err != nil {
					t.Fatal(err)
				}
				if err = data.Flush(base, ""); err != nil {
					t.Fatal(err)
				}
				_, active := data.InvocationTransaction()
				if active == nil {
					t.Fatal("no real transaction")
				}
				var count int
				if err = active.QueryRowContext(base, "SELECT COUNT(*) FROM children").Scan(&count); err != nil || count != 1 {
					t.Fatalf("write not physically flushed: %d/%v", count, err)
				}
				cause := errors.New("late failure after real write")
				ctx := base
				if cancelled {
					var stop context.CancelFunc
					ctx, stop = context.WithCancel(base)
					stop()
					cause = context.Canceled
				}
				if err = data.Complete(ctx, cause); !errors.Is(err, cause) {
					t.Fatalf("lost failure %v", err)
				}
				if external {
					if _, err = tx.ExecContext(base, "INSERT INTO caller_audit VALUES(2)"); err != nil {
						t.Fatalf("caller transaction completed by writer: %v", err)
					}
					if err = tx.Rollback(); err != nil {
						t.Fatal(err)
					}
				}
				if err = db.DB.QueryRowContext(base, "SELECT COUNT(*) FROM children").Scan(&count); err != nil || count != 0 {
					t.Fatalf("rollback children=%d/%v", count, err)
				}
				if err = db.DB.QueryRowContext(base, "SELECT COUNT(*) FROM caller_audit").Scan(&count); err != nil || count != 0 {
					t.Fatalf("caller rollback=%d/%v", count, err)
				}
				parents := sqParents(t, base, db)
				if len(parents) != 1 || *parents[0].Label != "original" {
					t.Fatal("auxiliary parent changed")
				}
				if len(in.Rows) != 3 || in.Rows[0] != nil || in.Rows[2] != nil {
					t.Fatal("rollback compacted original body")
				}
			})
		}
	}
}

// A late hook clears its own auxiliary slot.
type auxiliaryLateSelfClearProbe struct{ input *sqPlainInput }

func (p *auxiliaryLateSelfClearProbe) AfterQueue(context.Context, *sqPlainParent, h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	p.input.Rows[0] = nil
	return nil
}
func TestAuxiliaryNullLateHookClearingSelfMustReject(t *testing.T) {
	handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	in := queueInput(false)
	snapshot, err := handler.CaptureInput(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	program := snapshot.(*Program)
	if err = program.buildRecordFrames(context.Background(), nil, program.metadata.Root, reflect.ValueOf(in.Rows), nil); err != nil {
		t.Fatal(err)
	}
	frame := program.frames.Rows[0]
	frame.Hook = reflect.ValueOf(&auxiliaryLateSelfClearProbe{input: in})
	if err = program.callEntityHook(context.Background(), "AfterQueue", frame); err == nil {
		t.Fatal("late self-cleared auxiliary frame accepted after its hook")
	}
}

type auxiliaryLateChildProbe struct {
	input   *sqPlainInput
	replace bool
	reverse bool
}

func (p *auxiliaryLateChildProbe) AfterQueue(context.Context, *sqPlainChild, h.LifecycleContext[sqPlainChild, sqPlainParent, sqPlainOutput]) error {
	oldRows := p.input.Rows
	if p.replace {
		p.input.Rows = append([]*sqPlainParent(nil), p.input.Rows...)
	}
	if p.reverse {
		oldRows[0] = nil
	} else {
		p.input.Rows[0] = nil
	}
	return nil
}

type auxiliaryLateChildBinder struct {
	sqBinder
	input   *sqPlainInput
	replace bool
	reverse bool
}

func (b *auxiliaryLateChildBinder) Bind(_ context.Context, hook any) error {
	if p, ok := hook.(*auxiliaryLateChildProbe); ok {
		p.replace = b.replace
		p.reverse = b.reverse
		p.input = b.input
	}
	return nil
}
func TestAuxiliaryNullLateChildHookInvalidatingParentRollsBack(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')")...); err != nil {
		t.Fatal(err)
	}
	handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliaryLateChildProbe]()
	in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Children: []*sqPlainChild{{ID: ptr(11), Label: ptr("must roll back"), Has: &sqPlainChildHas{ID: true, Label: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
	snapshot, err := handler.CaptureInput(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	data := sqldml.NewData(db.DB)
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliaryLateChildBinder{sqBinder: sqBinder{data: data}, input: in}})
	if err == nil || !strings.Contains(err.Error(), "Rows[0]") {
		t.Fatalf("late invalidation accepted: %v", err)
	}
	if completeErr := data.Complete(ctx, err); !errors.Is(completeErr, err) {
		t.Fatalf("cause lost: %v", completeErr)
	}
	if len(sqChildren(t, ctx, db)) != 0 || *sqParents(t, ctx, db)[0].Label != "original" {
		t.Fatal("invalidated graph persisted")
	}
}
func TestAuxiliaryNullPhaseBoundaryRejectsPreviouslyVisitedSibling(t *testing.T) {
	handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	in := queueInput(false)
	snapshot, err := handler.CaptureInput(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	p := snapshot.(*Program)
	if err = p.buildRecordFrames(context.Background(), nil, p.metadata.Root, reflect.ValueOf(in.Rows), nil); err != nil {
		t.Fatal(err)
	}
	in.Rows[0] = nil
	if err = p.validateAuxiliaryTopology(); err == nil || !strings.Contains(err.Error(), "Rows[0]") {
		t.Fatal("invalid earlier subtree survived phase", err)
	}
}

type auxiliarySecondInitState struct {
	input                 *sqPlainInput
	rootCalls, childCalls int
	reverse               bool
	inPlace               bool
	dmlCalls              int
}
type auxiliarySecondInitParent struct{ state *auxiliarySecondInitState }

func (p *auxiliarySecondInitParent) Init(_ context.Context, _ *sqPlainParent, s h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	p.state.rootCalls++
	if s.Location == "Rows[0]" {
		p.state.input.Rows = append(p.state.input.Rows, &sqPlainParent{ID: ptr(2), Children: []*sqPlainChild{{ID: ptr(22), Label: ptr("never persist"), Has: &sqPlainChildHas{ID: true, Label: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}})
	} else {
		if p.state.inPlace {
			replacement := *p.state.input.Rows[1]
			replacement.Children = []*sqPlainChild{nil}
			p.state.input.Rows[1] = &replacement
		} else if p.state.reverse {
			old := p.state.input.Rows
			p.state.input.Rows = append([]*sqPlainParent(nil), old...)
			old[1] = nil
		} else {
			p.state.input.Rows[1] = nil
		}
	}
	return nil
}

type auxiliarySecondInitChild struct{ state *auxiliarySecondInitState }

func (p *auxiliarySecondInitChild) Init(_ context.Context, _ *sqPlainChild, s h.LifecycleContext[sqPlainChild, sqPlainParent, sqPlainOutput]) error {
	p.state.childCalls++
	if s.Location == "Rows[0].Children[0]" {
		row := p.state.input.Rows[0]
		row.Children = append(row.Children, &sqPlainChild{ID: ptr(22), Has: &sqPlainChildHas{ID: true}})
	} else {
		if p.state.inPlace {
			replacement := *p.state.input.Rows[0].Children[1]
			p.state.input.Rows[0].Children[1] = &replacement
		} else if p.state.reverse {
			old := p.state.input.Rows[0].Children
			p.state.input.Rows[0].Children = append([]*sqPlainChild(nil), old...)
			old[1] = nil
		} else {
			p.state.input.Rows[0].Children[1] = nil
		}
	}
	return nil
}

type auxiliarySecondInitBinder struct {
	sqBinder
	state *auxiliarySecondInitState
}

func (b *auxiliarySecondInitBinder) Lookup(ctx context.Context, key h.ValueKey) (any, bool, error) {
	if key == h.DMLKey {
		return &auxiliaryObservedDML{Data: b.data, calls: &b.state.dmlCalls}, true, nil
	}
	return b.sqBinder.Lookup(ctx, key)
}

func (b *auxiliarySecondInitBinder) Bind(_ context.Context, hook any) error {
	switch h := hook.(type) {
	case *auxiliarySecondInitParent:
		h.state = b.state
	case *auxiliarySecondInitChild:
		h.state = b.state
	}
	return nil
}
func TestAuxiliaryNullSecondInitClearedSlotsPreserveGraphAndHookCounts(t *testing.T) {
	for _, root := range []bool{true, false} {
		t.Run(fmt.Sprint("root=", root), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')", "INSERT INTO parents(id,name) VALUES(2,'other')")...); err != nil {
				t.Fatal(err)
			}
			handler, err := New(auxiliaryNullComponent(root), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
			state := &auxiliarySecondInitState{input: in}
			if root {
				handler.metadata.Root.HookType = reflect.TypeFor[auxiliarySecondInitParent]()
			} else {
				in.Rows[0].Children = []*sqPlainChild{{ID: ptr(11), Has: &sqPlainChildHas{ID: true}}}
				in.Rows[0].Has.Children = true
				handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliarySecondInitChild]()
			}
			snapshot, err := handler.CaptureInput(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			data := sqldml.NewData(db.DB)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliarySecondInitBinder{sqBinder: sqBinder{data: data}, state: state}})
			completeErr := data.Complete(ctx, err)
			if err != nil || completeErr != nil {
				t.Fatalf("execute=%v complete=%v", err, completeErr)
			}
			if root && (state.rootCalls != 2 || len(in.Rows) != 2 || in.Rows[1] != nil) {
				t.Fatalf("root topology/calls: %+v rows=%v", state, in.Rows)
			}
			if !root && (state.childCalls != 2 || len(in.Rows[0].Children) != 2 || in.Rows[0].Children[1] != nil || !in.Rows[0].Has.Children) {
				t.Fatalf("child topology/calls: %+v", state)
			}
			for _, frame := range snapshot.(*Program).frames.Rows {
				if root && strings.HasPrefix(frame.Location, "Rows[1]") || !root && strings.HasPrefix(frame.Location, "Rows[0].Children[1]") {
					t.Fatalf("stale second-pass frame: %s", frame.Location)
				}
			}
			if len(sqParents(t, ctx, db)) != 2 || len(sqChildren(t, ctx, db)) != 0 {
				t.Fatal("removed or auxiliary second-pass graph persisted")
			}
		})
	}
}

type auxiliarySiblingLateState struct {
	input  *sqPlainInput
	calls  int
	future bool
}
type auxiliarySiblingLateHook struct{ state *auxiliarySiblingLateState }

func (p *auxiliarySiblingLateHook) AfterQueue(_ context.Context, _ *sqPlainChild, s h.LifecycleContext[sqPlainChild, sqPlainParent, sqPlainOutput]) error {
	p.state.calls++
	if p.state.future && s.Location == "Rows[0].Children[0]" {
		p.state.input.Rows[1] = nil
	}
	if !p.state.future && s.Location == "Rows[1].Children[0]" {
		p.state.input.Rows[0] = nil
	}
	return nil
}

type auxiliarySiblingLateBinder struct {
	sqBinder
	state   *auxiliarySiblingLateState
	inserts int
}
type auxiliaryObservedDML struct {
	*sqldml.Data
	calls *int
}

func (d *auxiliaryObservedDML) Insert(table string, value any) error {
	*d.calls++
	return d.Data.Insert(table, value)
}
func (b *auxiliarySiblingLateBinder) Lookup(ctx context.Context, key h.ValueKey) (any, bool, error) {
	if key == h.DMLKey {
		return &auxiliaryObservedDML{Data: b.data, calls: &b.inserts}, true, nil
	}
	return b.sqBinder.Lookup(ctx, key)
}

func (b *auxiliarySiblingLateBinder) Bind(_ context.Context, hook any) error {
	if h, ok := hook.(*auxiliarySiblingLateHook); ok {
		h.state = b.state
	}
	return nil
}
func TestAuxiliaryNullRealQueueRejectsEarlierAndLaterSiblingInvalidation(t *testing.T) {
	for _, future := range []bool{true, false} {
		t.Run(fmt.Sprint("future=", future), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')", "INSERT INTO parents(id,name) VALUES(2,'other')")...); err != nil {
				t.Fatal(err)
			}
			handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliarySiblingLateHook]()
			in := &sqPlainInput{}
			for i := 1; i <= 2; i++ {
				in.Rows = append(in.Rows, &sqPlainParent{ID: ptr(i), Children: []*sqPlainChild{{ID: ptr(i * 11), Label: ptr("must roll back"), Has: &sqPlainChildHas{ID: true, Label: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}})
				in.CurrentRows = append(in.CurrentRows, &sqPlainParent{ID: ptr(i), Name: ptr("original")})
			}
			state := &auxiliarySiblingLateState{input: in, future: future}
			snapshot, err := handler.CaptureInput(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			data := sqldml.NewData(db.DB)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			binder := &auxiliarySiblingLateBinder{sqBinder: sqBinder{data: data}, state: state}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: binder})
			slot := 0
			wantCalls := 2
			if future {
				slot = 1
				wantCalls = 1
			}
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("Rows[%d]", slot)) {
				t.Fatalf("stale queue accepted: %v", err)
			}
			if state.calls != wantCalls {
				t.Fatalf("hooks=%d want=%d", state.calls, wantCalls)
			}
			if binder.inserts != wantCalls {
				t.Fatalf("DML calls=%d want=%d; stale child reached DML", binder.inserts, wantCalls)
			}
			if completeErr := data.Complete(ctx, err); !errors.Is(completeErr, err) {
				t.Fatalf("cause lost: %v", completeErr)
			}
			if len(sqChildren(t, ctx, db)) != 0 || len(sqParents(t, ctx, db)) != 2 {
				t.Fatal("late-invalidated mutation persisted")
			}
		})
	}
}

func TestAuxiliaryNullReallocatedRootLateHookRollsBack(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')")...); err != nil {
		t.Fatal(err)
	}
	handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliaryLateChildProbe]()
	in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Children: []*sqPlainChild{{ID: ptr(11), Label: ptr("must roll back"), Has: &sqPlainChildHas{ID: true, Label: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
	snapshot, err := handler.CaptureInput(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	data := sqldml.NewData(db.DB)
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliaryLateChildBinder{sqBinder: sqBinder{data: data}, input: in, replace: true}})
	if err == nil || !strings.Contains(err.Error(), "Rows[0]") {
		t.Fatalf("late invalidation accepted: %v", err)
	}
	if completeErr := data.Complete(ctx, err); !errors.Is(completeErr, err) {
		t.Fatalf("cause lost: %v", completeErr)
	}
	if len(sqChildren(t, ctx, db)) != 0 || *sqParents(t, ctx, db)[0].Label != "original" {
		t.Fatal("invalidated graph persisted")
	}
}

type auxiliaryNestedReallocateProbe struct {
	input   *sqPlainInput
	reverse bool
}

func (p *auxiliaryNestedReallocateProbe) AfterSequence(context.Context, *sqPlainChild, h.LifecycleContext[sqPlainChild, sqPlainParent, sqPlainOutput]) error {
	oldChildren := p.input.Rows[0].Children
	p.input.Rows[0].Children = append([]*sqPlainChild(nil), p.input.Rows[0].Children...)
	if p.reverse {
		oldChildren[0] = nil
	} else {
		p.input.Rows[0].Children[0] = nil
	}
	return nil
}

type auxiliaryNestedReallocateBinder struct {
	sqBinder
	input   *sqPlainInput
	reverse bool
}

func (b *auxiliaryNestedReallocateBinder) Bind(_ context.Context, hook any) error {
	if p, ok := hook.(*auxiliaryNestedReallocateProbe); ok {
		p.reverse = b.reverse
		p.input = b.input
	}
	return nil
}
func TestAuxiliaryNullReallocatedNestedCollectionRejectsLateHook(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')")...); err != nil {
		t.Fatal(err)
	}
	handler, err := New(auxiliaryNullComponent(false), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliaryNestedReallocateProbe]()
	in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Children: []*sqPlainChild{{ID: ptr(11), Has: &sqPlainChildHas{ID: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
	snapshot, err := handler.CaptureInput(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	data := sqldml.NewData(db.DB)
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliaryNestedReallocateBinder{sqBinder: sqBinder{data: data}, input: in}})
	if err == nil || !strings.Contains(err.Error(), "Rows[0].Children[0]") {
		t.Fatalf("reallocated null accepted: %v", err)
	}
	if e := data.Complete(ctx, err); !errors.Is(e, err) {
		t.Fatalf("cause lost: %v", e)
	}
	if len(sqChildren(t, ctx, db)) != 0 || *sqParents(t, ctx, db)[0].Label != "original" {
		t.Fatal("late-invalidated nested graph persisted")
	}
}

func TestAuxiliaryNullSecondInitSkipsRemovedMatchedChildConcurrency(t *testing.T) {
	for _, root := range []bool{true} {
		t.Run(fmt.Sprint("root=", root), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')", "INSERT INTO parents(id,name) VALUES(2,'other')", "INSERT INTO children(id,parent_id,label) VALUES(22,2,'token-original')")...); err != nil {
				t.Fatal(err)
			}
			handler, err := New(auxiliaryNullComponent(root), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
			in.CurrentRows = append(in.CurrentRows, &sqPlainParent{ID: ptr(2), Children: []*sqPlainChild{{ID: ptr(22), ParentID: ptr(2), Label: ptr("token-original")}}})
			in.CurrentChildren = []*sqPlainChild{{ID: ptr(22), ParentID: ptr(2), Label: ptr("token-original")}}
			childRecord := handler.metadata.Root.Relations[0].Child
			for i := range childRecord.Fields {
				if childRecord.Fields[i].Name == "Label" {
					childRecord.ConcurrencyToken = &childRecord.Fields[i]
				}
			}
			if childRecord.ConcurrencyToken == nil {
				t.Fatal("missing token test metadata")
			}
			state := &auxiliarySecondInitState{input: in}
			if root {
				handler.metadata.Root.HookType = reflect.TypeFor[auxiliarySecondInitParent]()
			} else {
				in.Rows[0].Children = []*sqPlainChild{{ID: ptr(11), Has: &sqPlainChildHas{ID: true}}}
				in.Rows[0].Has.Children = true
				handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliarySecondInitChild]()
			}
			snapshot, err := handler.CaptureInput(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			data := sqldml.NewData(db.DB)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliarySecondInitBinder{sqBinder: sqBinder{data: data}, state: state}})
			completeErr := data.Complete(ctx, err)
			if err != nil || completeErr != nil {
				t.Fatalf("execute=%v complete=%v", err, completeErr)
			}
			if root && (state.rootCalls != 2 || len(in.Rows) != 2 || in.Rows[1] != nil) {
				t.Fatalf("root topology/calls: %+v rows=%v", state, in.Rows)
			}
			if !root && (state.childCalls != 2 || len(in.Rows[0].Children) != 2 || in.Rows[0].Children[1] != nil || !in.Rows[0].Has.Children) {
				t.Fatalf("child topology/calls: %+v", state)
			}
			for _, frame := range snapshot.(*Program).frames.Rows {
				if root && strings.HasPrefix(frame.Location, "Rows[1]") || !root && strings.HasPrefix(frame.Location, "Rows[0].Children[1]") {
					t.Fatalf("stale second-pass frame: %s", frame.Location)
				}
			}
			if len(sqParents(t, ctx, db)) != 2 || len(sqChildren(t, ctx, db)) != 1 {
				t.Fatal("removed or auxiliary second-pass graph persisted")
			}
		})
	}
}

func TestAuxiliaryNullDetachedRetainedRootSlotRejectsLateHook(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')")...); err != nil {
		t.Fatal(err)
	}
	handler, err := New(auxiliaryNullComponent(true), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliaryLateChildProbe]()
	in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Children: []*sqPlainChild{{ID: ptr(11), Label: ptr("must roll back"), Has: &sqPlainChildHas{ID: true, Label: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
	snapshot, err := handler.CaptureInput(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	data := sqldml.NewData(db.DB)
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliaryLateChildBinder{sqBinder: sqBinder{data: data}, input: in, replace: true, reverse: true}})
	if err == nil || !strings.Contains(err.Error(), "Rows[0]") {
		t.Fatalf("late invalidation accepted: %v", err)
	}
	if completeErr := data.Complete(ctx, err); !errors.Is(completeErr, err) {
		t.Fatalf("cause lost: %v", completeErr)
	}
	if len(sqChildren(t, ctx, db)) != 0 || *sqParents(t, ctx, db)[0].Label != "original" {
		t.Fatal("invalidated graph persisted")
	}
}

func TestAuxiliaryNullDetachedRetainedNestedSlotRejectsLateHook(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')")...); err != nil {
		t.Fatal(err)
	}
	handler, err := New(auxiliaryNullComponent(false), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliaryNestedReallocateProbe]()
	in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Children: []*sqPlainChild{{ID: ptr(11), Has: &sqPlainChildHas{ID: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
	snapshot, err := handler.CaptureInput(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	data := sqldml.NewData(db.DB)
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliaryNestedReallocateBinder{sqBinder: sqBinder{data: data}, input: in, reverse: true}})
	if err == nil || !strings.Contains(err.Error(), "Rows[0].Children[0]") {
		t.Fatalf("reallocated null accepted: %v", err)
	}
	if e := data.Complete(ctx, err); !errors.Is(e, err) {
		t.Fatalf("cause lost: %v", e)
	}
	if len(sqChildren(t, ctx, db)) != 0 || *sqParents(t, ctx, db)[0].Label != "original" {
		t.Fatal("late-invalidated nested graph persisted")
	}
}

func TestAuxiliaryNullSecondInitRejectsDetachedRetainedSlots(t *testing.T) {
	for _, root := range []bool{true, false} {
		t.Run(fmt.Sprint("root=", root), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')", "INSERT INTO parents(id,name) VALUES(2,'other')")...); err != nil {
				t.Fatal(err)
			}
			handler, err := New(auxiliaryNullComponent(root), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
			state := &auxiliarySecondInitState{input: in, reverse: true}
			if root {
				handler.metadata.Root.HookType = reflect.TypeFor[auxiliarySecondInitParent]()
			} else {
				in.Rows[0].Children = []*sqPlainChild{{ID: ptr(11), Has: &sqPlainChildHas{ID: true}}}
				in.Rows[0].Has.Children = true
				handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliarySecondInitChild]()
			}
			snapshot, err := handler.CaptureInput(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			data := sqldml.NewData(db.DB)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliarySecondInitBinder{sqBinder: sqBinder{data: data}, state: state}})
			if err == nil || !strings.Contains(err.Error(), "changed after framing") {
				t.Fatalf("detached retained slot accepted: %v", err)
			}
			if e := data.Complete(ctx, err); !errors.Is(e, err) {
				t.Fatalf("cause lost: %v", e)
			}
			if root && (state.rootCalls != 2 || len(in.Rows) != 2 || in.Rows[1] == nil) {
				t.Fatalf("live root changed or duplicate Init: %+v", state)
			}
			if !root && (state.childCalls != 2 || len(in.Rows[0].Children) != 2 || in.Rows[0].Children[1] == nil) {
				t.Fatalf("live nested changed or duplicate Init: %+v", state)
			}
			if len(sqParents(t, ctx, db)) != 2 || len(sqChildren(t, ctx, db)) != 0 {
				t.Fatal("detached graph persisted")
			}
		})
	}
}

type auxiliaryInPlaceReplacementState struct {
	input    *sqPlainInput
	dmlCalls int
}
type auxiliaryInPlaceRootHook struct {
	state *auxiliaryInPlaceReplacementState
}

func (p *auxiliaryInPlaceRootHook) AfterSequence(context.Context, *sqPlainParent, h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	replacement := *p.state.input.Rows[0]
	replacement.Children = []*sqPlainChild{nil}
	p.state.input.Rows[0] = &replacement
	return nil
}

type auxiliaryInPlaceChildHook struct {
	state *auxiliaryInPlaceReplacementState
}

func (p *auxiliaryInPlaceChildHook) AfterSequence(context.Context, *sqPlainChild, h.LifecycleContext[sqPlainChild, sqPlainParent, sqPlainOutput]) error {
	replacement := *p.state.input.Rows[0].Children[0]
	p.state.input.Rows[0].Children[0] = &replacement
	return nil
}

type auxiliaryInPlaceBinder struct {
	sqBinder
	state *auxiliaryInPlaceReplacementState
}

func (b *auxiliaryInPlaceBinder) Bind(_ context.Context, hook any) error {
	switch p := hook.(type) {
	case *auxiliaryInPlaceRootHook:
		p.state = b.state
	case *auxiliaryInPlaceChildHook:
		p.state = b.state
	}
	return nil
}
func (b *auxiliaryInPlaceBinder) Lookup(ctx context.Context, key h.ValueKey) (any, bool, error) {
	if key == h.DMLKey {
		return &auxiliaryObservedDML{Data: b.data, calls: &b.state.dmlCalls}, true, nil
	}
	return b.sqBinder.Lookup(ctx, key)
}
func TestAuxiliaryNullRejectsInPlaceEntityReplacementBeforeDML(t *testing.T) {
	for _, root := range []bool{true, false} {
		t.Run(fmt.Sprint("root=", root), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')")...); err != nil {
				t.Fatal(err)
			}
			handler, err := New(auxiliaryNullComponent(root), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			if root {
				handler.metadata.Root.HookType = reflect.TypeFor[auxiliaryInPlaceRootHook]()
			} else {
				handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliaryInPlaceChildHook]()
			}
			in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Children: []*sqPlainChild{{ID: ptr(11), Label: ptr("must not queue"), Has: &sqPlainChildHas{ID: true, Label: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
			state := &auxiliaryInPlaceReplacementState{input: in}
			snapshot, err := handler.CaptureInput(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			data := sqldml.NewData(db.DB)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliaryInPlaceBinder{sqBinder: sqBinder{data: data}, state: state}})
			if err == nil || !strings.Contains(err.Error(), "changed after framing") {
				t.Fatalf("replacement implicitly rebound: %v", err)
			}
			if state.dmlCalls != 0 {
				t.Fatalf("stale child reached DML: %d", state.dmlCalls)
			}
			if e := data.Complete(ctx, err); !errors.Is(e, err) {
				t.Fatalf("cause lost: %v", e)
			}
			if len(sqChildren(t, ctx, db)) != 0 || *sqParents(t, ctx, db)[0].Label != "original" {
				t.Fatal("replaced graph persisted")
			}
		})
	}
}

func TestAuxiliaryNullSecondInitRejectsInPlaceReplacement(t *testing.T) {
	for _, root := range []bool{true, false} {
		t.Run(fmt.Sprint("root=", root), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')", "INSERT INTO parents(id,name) VALUES(2,'other')")...); err != nil {
				t.Fatal(err)
			}
			handler, err := New(auxiliaryNullComponent(root), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}}}
			state := &auxiliarySecondInitState{input: in, inPlace: true}
			if root {
				handler.metadata.Root.HookType = reflect.TypeFor[auxiliarySecondInitParent]()
			} else {
				in.Rows[0].Children = []*sqPlainChild{{ID: ptr(11), Has: &sqPlainChildHas{ID: true}}}
				in.Rows[0].Has.Children = true
				handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliarySecondInitChild]()
			}
			snapshot, err := handler.CaptureInput(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			data := sqldml.NewData(db.DB)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliarySecondInitBinder{sqBinder: sqBinder{data: data}, state: state}})
			if err == nil || !strings.Contains(err.Error(), "changed after framing") {
				t.Fatalf("detached retained slot accepted: %v", err)
			}
			if state.dmlCalls != 0 {
				t.Fatalf("stale descendant reached DML: %d", state.dmlCalls)
			}
			if e := data.Complete(ctx, err); !errors.Is(e, err) {
				t.Fatalf("cause lost: %v", e)
			}
			if root && (state.rootCalls != 2 || len(in.Rows) != 2 || in.Rows[1] == nil) {
				t.Fatalf("live root changed or duplicate Init: %+v", state)
			}
			if !root && (state.childCalls != 2 || len(in.Rows[0].Children) != 2 || in.Rows[0].Children[1] == nil) {
				t.Fatalf("live nested changed or duplicate Init: %+v", state)
			}
			if len(sqParents(t, ctx, db)) != 2 || len(sqChildren(t, ctx, db)) != 0 {
				t.Fatal("detached graph persisted")
			}
		})
	}
}

type auxiliaryFirstInitState struct {
	input           *sqPlainInput
	root, copyOnly  bool
	calls, dmlCalls int
}
type auxiliaryFirstInitParent struct{ state *auxiliaryFirstInitState }

func (p *auxiliaryFirstInitParent) Init(_ context.Context, _ *sqPlainParent, s h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	p.state.calls++
	if s.Location == "Rows[1]" {
		if p.state.copyOnly {
			p.state.input.Rows = append([]*sqPlainParent(nil), p.state.input.Rows...)
		} else {
			replacement := *p.state.input.Rows[0]
			p.state.input.Rows[0] = &replacement
		}
	}
	return nil
}

type auxiliaryFirstInitChild struct{ state *auxiliaryFirstInitState }

func (p *auxiliaryFirstInitChild) Init(_ context.Context, _ *sqPlainChild, s h.LifecycleContext[sqPlainChild, sqPlainParent, sqPlainOutput]) error {
	p.state.calls++
	if s.Location == "Rows[0].Children[1]" {
		row := p.state.input.Rows[0]
		if p.state.copyOnly {
			row.Children = append([]*sqPlainChild(nil), row.Children...)
		} else {
			replacement := *row.Children[0]
			row.Children[0] = &replacement
		}
	}
	return nil
}

type auxiliaryFirstInitBinder struct {
	sqBinder
	state *auxiliaryFirstInitState
}

func (b *auxiliaryFirstInitBinder) Lookup(ctx context.Context, key h.ValueKey) (any, bool, error) {
	if key == h.DMLKey {
		return &auxiliaryObservedDML{Data: b.data, calls: &b.state.dmlCalls}, true, nil
	}
	return b.sqBinder.Lookup(ctx, key)
}
func (b *auxiliaryFirstInitBinder) Bind(_ context.Context, hook any) error {
	switch h := hook.(type) {
	case *auxiliaryFirstInitParent:
		h.state = b.state
	case *auxiliaryFirstInitChild:
		h.state = b.state
	}
	return nil
}
func TestAuxiliaryNullFirstInitPastSiblingIdentity(t *testing.T) {
	for _, root := range []bool{true, false} {
		for _, copyOnly := range []bool{false, true} {
			t.Run(fmt.Sprint("root=", root, "/copy=", copyOnly), func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'original')", "INSERT INTO parents(id,name) VALUES(2,'other')")...); err != nil {
					t.Fatal(err)
				}
				handler, err := New(auxiliaryNullComponent(root), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
				if err != nil {
					t.Fatal(err)
				}
				in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true, Children: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("original")}, {ID: ptr(2), Name: ptr("other")}}}
				if root {
					in.Rows[0].Children = []*sqPlainChild{{ID: ptr(11), Label: ptr("first"), Has: &sqPlainChildHas{ID: true, Label: true}}}
					in.Rows = append(in.Rows, &sqPlainParent{ID: ptr(2), Children: []*sqPlainChild{{ID: ptr(22), Label: ptr("second"), Has: &sqPlainChildHas{ID: true, Label: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}})
					handler.metadata.Root.HookType = reflect.TypeFor[auxiliaryFirstInitParent]()
				} else {
					in.Rows[0].Children = []*sqPlainChild{{ID: ptr(11), Has: &sqPlainChildHas{ID: true}}, {ID: ptr(22), Has: &sqPlainChildHas{ID: true}}}
					handler.metadata.Root.Relations[0].Child.HookType = reflect.TypeFor[auxiliaryFirstInitChild]()
				}
				state := &auxiliaryFirstInitState{input: in, root: root, copyOnly: copyOnly}
				snapshot, err := handler.CaptureInput(ctx, in)
				if err != nil {
					t.Fatal(err)
				}
				data := sqldml.NewData(db.DB)
				if err = data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &auxiliaryFirstInitBinder{sqBinder: sqBinder{data: data}, state: state}})
				completeErr := data.Complete(ctx, err)
				if copyOnly {
					if err != nil || completeErr != nil {
						t.Fatalf("same-pointer collection copy rejected: %v / %v", err, completeErr)
					}
				} else {
					if err == nil || !strings.Contains(err.Error(), "changed after framing") {
						t.Fatalf("past-sibling replacement accepted: %v", err)
					}
					if state.dmlCalls != 0 {
						t.Fatalf("replacement reached DML: %d", state.dmlCalls)
					}
				}
				if state.calls != 2 {
					t.Fatalf("replacement initialized again or existing hook repeated: %d", state.calls)
				}
				wantChildren := 0
				if copyOnly && root {
					wantChildren = 2
				}
				if len(sqParents(t, ctx, db)) != 2 || len(sqChildren(t, ctx, db)) != wantChildren {
					t.Fatal("unexpected physical graph")
				}
			})
		}
	}
}
