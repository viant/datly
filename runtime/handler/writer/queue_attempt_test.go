package writer

import (
	"context"
	"errors"
	"fmt"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	h "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
	"reflect"
	"sync"
	"testing"
)

type queueProbe struct {
	passingAggregateProbe
	events                []h.QueueAttemptEvent
	timeline              []string
	after                 int
	afterFailure          error
	mutate, panicObserver bool
}

func (p *queueProbe) ObserveQueueAttempt(_ context.Context, e h.QueueAttemptEvent) {
	p.events = append(p.events, e)
	p.timeline = append(p.timeline, string(e.Boundary)+":"+e.Location)
	if p.mutate && e.Boundary == h.PhaseBegin {
		switch row := e.Row.(type) {
		case *sqPlainParent:
			if row.Name != nil {
				*row.Name = "observer"
			}
			row.Has.Name = false
			if len(row.Children) > 0 {
				row.Children[0].Label = ptr("observer")
			}
		case *sqPlainChild:
			row.Label = ptr("observer")
			row.Has.Label = false
		}
		switch row := e.Previous.(type) {
		case *sqPlainParent:
			row.Name = ptr("observer")
		case *sqPlainChild:
			row.Label = ptr("observer")
		}
		if fields, ok := e.Presence.(fieldSet); ok {
			fields["Name"] = false
			fields["Label"] = false
		}
	}
	if p.panicObserver {
		panic("observer private detail")
	}
}
func (p *queueProbe) AfterQueue(_ context.Context, _ *sqPlainParent, _ h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	p.after++
	return p.afterFailure
}

type queueCaps struct {
	finalValidationCapabilities
	probe                *queueProbe
	writes               []string
	flushes              int
	failAt               int
	failure              error
	missingDML, panicDML bool
	allocationFailure    error
	cancel               context.CancelFunc
}

func (c *queueCaps) Bind(_ context.Context, hook any) error {
	if probe, ok := hook.(*queueProbe); ok {
		c.probe = probe
	}
	return nil
}
func (c *queueCaps) Lookup(ctx context.Context, key h.ValueKey) (any, bool, error) {
	if key == h.FlusherKey {
		return c, true, nil
	}
	if key == h.DMLKey {
		if c.missingDML {
			return nil, false, nil
		}
		return c, true, nil
	}
	if key == h.SequencerKey {
		return c, true, nil
	}
	return c.finalValidationCapabilities.Lookup(ctx, key)
}
func (c *queueCaps) Allocate(context.Context, string, any, string) error {
	c.allocations++
	return c.allocationFailure
}
func (c *queueCaps) write(op, table string, row any) error {
	id := 0
	switch row := row.(type) {
	case *sqPlainParent:
		if row.ID != nil {
			id = *row.ID
		}
	case *sqPlainChild:
		if row.ID != nil {
			id = *row.ID
		}
	}
	entry := fmt.Sprintf("%s:%s:%d", op, table, id)
	c.writes = append(c.writes, entry)
	if c.probe != nil {
		c.probe.timeline = append(c.probe.timeline, entry)
	}
	if c.failAt == len(c.writes) {
		if c.cancel != nil {
			c.cancel()
		}
		if c.panicDML {
			panic("original queue panic")
		}
		return c.failure
	}
	return nil
}
func (c *queueCaps) Insert(table string, row any) error { return c.write("insert", table, row) }
func (c *queueCaps) Update(table string, row any) error { return c.write("update", table, row) }
func (c *queueCaps) Delete(table string, row any) error { return c.write("delete", table, row) }

func queueInput(mixed bool) *sqPlainInput {
	in := &sqPlainInput{}
	for i := 1; i <= 2; i++ {
		row := &sqPlainParent{ID: ptr(i), Name: ptr("new"), Has: &sqPlainParentHas{ID: true, Name: true}, Children: []*sqPlainChild{{ID: ptr(i*10 + 1), ParentID: ptr(i), Label: ptr("new"), Has: &sqPlainChildHas{ID: true, Label: true}}}}
		if mixed && i == 1 {
			row.Name = nil
			row.Has.Name = false
		}
		if mixed && i == 2 {
			row.Children[0].Label = nil
			row.Children[0].Has.Label = false
		}
		in.Rows = append(in.Rows, row)
		in.CurrentRows = append(in.CurrentRows, &sqPlainParent{ID: ptr(i), Name: ptr("old")})
		in.CurrentChildren = append(in.CurrentChildren, &sqPlainChild{ID: ptr(i*10 + 1), ParentID: ptr(i), Label: ptr("old")})
	}
	return in
}
func queueHandler(t *testing.T) *Handler {
	t.Helper()
	handler, err := New(sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", ""), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.HookType = reflect.TypeFor[queueProbe]()
	return handler
}
func executeQueue(t *testing.T, ctx context.Context, handler *Handler, in *sqPlainInput, probe *queueProbe, caps *queueCaps) (*Program, error) {
	t.Helper()
	// Exercise the queue-only observer adapter used by engine attempts.
	observer := &queuePhaseObserver{hook: reflect.ValueOf(probe)}
	ctx, _ = rhandler.WithPhaseObserver(ctx, observer, 88, 2)
	snapshot, err := handler.CaptureInput(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: caps})
	return snapshot.(*Program), err
}
func TestQueueAttemptMixedTraversalAndDetachedEvidence(t *testing.T) {
	in := queueInput(true)
	probe := &queueProbe{}
	caps := &queueCaps{}
	program, err := executeQueue(t, context.Background(), queueHandler(t), in, probe, caps)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"begin:Rows[0]", "end:Rows[0]", "begin:Rows[0].Children[0]", "update:children:11", "end:Rows[0].Children[0]", "begin:Rows[1]", "update:parents:2", "end:Rows[1]", "begin:Rows[1].Children[0]", "end:Rows[1].Children[0]"}
	if !reflect.DeepEqual(probe.timeline, want) || len(program.actions.Rows) != 2 || caps.starts != 1 || caps.allocations != 0 || probe.after != 1 {
		t.Fatalf("timeline=%v actions=%d starts=%d after=%d", probe.timeline, len(program.actions.Rows), caps.starts, probe.after)
	}
	for i, e := range probe.events {
		if e.InvocationID != 88 || e.Attempt != 2 || e.Position != i/2 || e.Operation != h.WriteUpdate || e.EvidenceError != nil || !e.Original.Available() || e.Previous == nil {
			t.Fatalf("event=%+v", e)
		}
		if i%2 == 0 {
			if e.Result != "" || e.Boundary != h.PhaseBegin {
				t.Fatal(e)
			}
		} else {
			want := h.QueueQueued
			if i == 1 || i == 7 {
				want = h.QueueNoopCompleted
			}
			if e.Result != want {
				t.Fatal(e)
			}
		}
	}
	// Evidence changes in entry callbacks cannot affect persistence or terminal evidence.
	in = queueInput(false)
	probe = &queueProbe{mutate: true}
	caps = &queueCaps{}
	program, err = executeQueue(t, context.Background(), queueHandler(t), in, probe, caps)
	if err != nil {
		t.Fatal(err)
	}
	if len(program.actions.Rows) != 4 || len(caps.writes) != 4 || *in.Rows[0].Name != "new" || !in.Rows[0].Has.Name || *in.Rows[0].Children[0].Label != "new" || *in.CurrentRows[0].Name != "old" {
		t.Fatal("observer changed working/database graph")
	}
	probe.mutate = false
	// Fresh terminal clones were created, rather than reusing mutated entry evidence.
	if *probe.events[1].Row.(*sqPlainParent).Name != "new" || *probe.events[1].Previous.(*sqPlainParent).Name != "old" || !probe.events[1].Presence.Has("Name") {
		t.Fatal("entry mutations leaked into terminal evidence")
	}
	if probe.events[0].Row == probe.events[1].Row || probe.events[0].Previous == probe.events[1].Previous {
		t.Fatal("boundary evidence shared")
	}
}
func TestQueueAttemptNoopOnlyAndPreviousNullEvidence(t *testing.T) {
	for _, nullPrevious := range []bool{false, true} {
		in := queueInput(true)
		in.Rows = in.Rows[:1]
		in.CurrentRows = in.CurrentRows[:1]
		in.Rows[0].Children[0].Label = nil
		in.Rows[0].Children[0].Has.Label = false
		if nullPrevious {
			in.CurrentRows[0].Name = nil
			in.CurrentChildren[0].Label = nil
		}
		probe := &queueProbe{}
		caps := &queueCaps{missingDML: true}
		program, err := executeQueue(t, context.Background(), queueHandler(t), in, probe, caps)
		if err != nil {
			t.Fatal(err)
		}
		if caps.starts != 0 || caps.allocations != 0 || len(caps.writes) != 0 || probe.after != 0 || len(program.actions.Rows) != 0 || len(probe.events) != 4 {
			t.Fatal("noop had physical side effects")
		}
		e := probe.events[0]
		if e.Disposition != h.QueueNoop || e.Original.Has("Name") || e.Row.(*sqPlainParent).Name != nil || e.PreviousFields.Has("unknown") || !e.PreviousFields.Has("Name") || (e.Previous.(*sqPlainParent).Name == nil) != nullPrevious {
			t.Fatal("insufficient conditional diff evidence")
		}
	}
}
func TestQueueAttemptFailurePrefixes(t *testing.T) {
	failure := errors.New("native queue failure")
	for _, at := range []int{1, 2, 3} {
		probe := &queueProbe{}
		caps := &queueCaps{failAt: at, failure: failure}
		_, err := executeQueue(t, context.Background(), queueHandler(t), queueInput(false), probe, caps)
		if !errors.Is(err, failure) || len(probe.events) != 2*at || len(caps.writes) != at || probe.events[2*at-1].Result != h.QueueFailed || !errors.Is(probe.events[2*at-1].Cause, failure) {
			t.Fatalf("at=%d error=%v events=%+v", at, err, probe.events)
		}
	}
	// Capability resolution belongs to the first reached physical item, after prior noops.
	probe := &queueProbe{}
	caps := &queueCaps{missingDML: true}
	_, err := executeQueue(t, context.Background(), queueHandler(t), queueInput(true), probe, caps)
	if err == nil || len(probe.events) != 4 || probe.events[3].Location != "Rows[0].Children[0]" || probe.events[3].Result != h.QueueFailed || len(caps.writes) != 0 {
		t.Fatal("capability failure position lost")
	}
	probe = &queueProbe{afterFailure: failure}
	caps = &queueCaps{}
	_, err = executeQueue(t, context.Background(), queueHandler(t), queueInput(false), probe, caps)
	if !errors.Is(err, failure) || len(probe.events) != 2 || len(caps.writes) != 1 || probe.events[1].Result != h.QueueFailed || !probe.events[1].Queued {
		t.Fatal("AfterQueue failure prefix changed")
	}
}
func TestQueueAttemptPrequeueFailuresHaveNoAttempts(t *testing.T) {
	failure := errors.New("allocator failed")
	for _, allocation := range []bool{false, true} {
		in := &sqPlainInput{Rows: []*sqPlainParent{{Name: ptr("new"), Has: &sqPlainParentHas{Name: true}}}}
		probe := &queueProbe{}
		caps := &queueCaps{}
		if allocation {
			caps.allocationFailure = failure
		} else {
			caps.failFinal = true
		}
		_, err := executeQueue(t, context.Background(), queueHandler(t), in, probe, caps)
		if err == nil || len(probe.events) != 0 || len(caps.writes) != 0 {
			t.Fatal("prequeue failure fabricated attempts")
		}
	}
}

type queueLog struct {
	errors int
	panics bool
}

func (*queueLog) Debug(string, ...any) {}
func (*queueLog) Info(string, ...any)  {}
func (*queueLog) Warn(string, ...any)  {}
func (l *queueLog) Error(string, ...any) {
	l.errors++
	if l.panics {
		panic("logger panic")
	}
}
func TestQueueAttemptCancellationAndPanicIsolation(t *testing.T) {
	for _, loggerPanic := range []bool{false, true} {
		log := &queueLog{panics: loggerPanic}
		probe := &queueProbe{panicObserver: true}
		caps := &queueCaps{}
		_, err := executeQueue(t, xlogger.WithContext(context.Background(), log), queueHandler(t), queueInput(false), probe, caps)
		if err != nil || len(caps.writes) != 4 || log.errors != 1 {
			t.Fatal("observer/logger panic controlled execution")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := &queueProbe{}
	caps := &queueCaps{failAt: 2, failure: context.Canceled, cancel: cancel}
	_, err := executeQueue(t, ctx, queueHandler(t), queueInput(false), probe, caps)
	if !errors.Is(err, context.Canceled) || len(probe.events) != 4 || probe.events[3].Result != h.QueueCanceled {
		t.Fatal("cancellation prefix lost")
	}
	probe = &queueProbe{}
	caps = &queueCaps{failAt: 2, panicDML: true}
	var caught any
	func() {
		defer func() { caught = recover() }()
		_, _ = executeQueue(t, context.Background(), queueHandler(t), queueInput(false), probe, caps)
	}()
	if caught != "original queue panic" || len(probe.events) != 4 || probe.events[3].Result != h.QueuePanicked || probe.events[3].Cause == nil {
		t.Fatal("execution panic lost")
	}
}
func TestQueueAttemptDeleteOrderAndOptOutStability(t *testing.T) {
	var physical []string
	for _, observed := range []bool{false, true} {
		handler := queueHandler(t)
		if !observed {
			handler.metadata.Root.HookType = nil
		}
		in := queueInput(false)
		for _, row := range in.Rows {
			row.Remove = true
			row.Has.Remove = true
			row.Children[0].Remove = true
			row.Children[0].Has.Remove = true
		}
		caps := &queueCaps{}
		probe := &queueProbe{}
		if observed {
			_, err := executeQueue(t, context.Background(), handler, in, probe, caps)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			_, err := handler.Execute(context.Background(), rhandler.Invocation{Input: in, Binder: caps})
			if err != nil {
				t.Fatal(err)
			}
		}
		if !observed {
			physical = caps.writes
		} else if !reflect.DeepEqual(physical, caps.writes) || len(probe.events) != 8 {
			t.Fatalf("physical order changed: %v/%v", physical, caps.writes)
		}
	}
	want := []string{"delete:children:21", "delete:parents:2", "delete:children:11", "delete:parents:1"}
	if !reflect.DeepEqual(physical, want) {
		t.Fatal(physical)
	}
}
func TestQueueAttemptParallelIsolationAndFreshAttemptState(t *testing.T) {
	handler := queueHandler(t)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in := queueInput(true)
			caps := &queueCaps{}
			ctx := context.Background()
			observer := handler.NewPhaseObserver()
			ctx, scope := rhandler.WithPhaseObserver(ctx, observer, 0, 0)
			snap, err := handler.CaptureInput(ctx, in)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snap, Binder: caps})
			if err != nil {
				t.Error(err)
				return
			}
			probe := snap.(*Program).hook.Interface().(*queueProbe)
			if len(probe.events) != 8 || probe.events[0].InvocationID != scope.InvocationID() {
				t.Error("cross-request state")
			}
		}()
	}
	wg.Wait()
	first := handler.NewPhaseObserver()
	second := handler.NewPhaseObserver()
	if first == second {
		t.Fatal("attempt object reused")
	}
	for attempt, observer := range []h.PhaseObserver{first, second} {
		ctx, _ := rhandler.WithPhaseObserver(context.Background(), observer, 77, attempt)
		in := queueInput(true)
		snap, err := handler.CaptureInput(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snap, Binder: &queueCaps{}})
		if err != nil {
			t.Fatal(err)
		}
		probe := snap.(*Program).hook.Interface().(*queueProbe)
		for _, e := range probe.events {
			if e.InvocationID != 77 || e.Attempt != attempt {
				t.Fatal("retry identity lost")
			}
		}
	}
}

func TestQueueAttemptPreparationFailureAndCauseIsolation(t *testing.T) {
	probe := &queueProbe{}
	record := &Record{Path: "Rows", Table: "parents", EntityType: reflect.TypeFor[sqPlainParent](), ConcurrencyToken: &Field{Name: "Name"}}
	row := &sqPlainParent{ID: ptr(1), Name: ptr("old"), Has: &sqPlainParentHas{ID: true}}
	frame := &Frame{Record: record, Entity: reflect.ValueOf(row), Location: "Rows[0]", Fields: livePresence(record, reflect.ValueOf(row).Elem()), Original: originalPresence{presence: snapshotPresence(record, reflect.ValueOf(row).Elem()), available: true}}
	action := &Action{Kind: h.WriteUpdate, Entity: frame.Entity, frame: frame}
	program := &Program{input: &sqPlainInput{}, metadata: &Metadata{Root: record}, hook: reflect.ValueOf(probe), frames: &MutationFrames{Rows: []*Frame{frame}}, actions: &MutationActions{Rows: []*Action{action}}, queueItems: []*Action{action}}
	caps := &queueCaps{}
	err := program.queue(context.Background(), caps)
	var conflict *h.Conflict
	if !errors.As(err, &conflict) || len(probe.events) != 2 || len(caps.writes) != 0 || probe.events[1].Result != h.QueueFailed || !errors.Is(probe.events[1].Cause, err) {
		t.Fatal("preparation failure lost")
	}
	var observerConflict *h.Conflict
	if errors.As(probe.events[1].Cause, &observerConflict) {
		t.Fatal("observer can mutate controlling error")
	}
}

type queueRecorder struct{ events []h.QueueAttemptEvent }

func (p *queueRecorder) ObserveQueueAttempt(_ context.Context, e h.QueueAttemptEvent) {
	p.events = append(p.events, e)
}
func TestQueueAttemptDoesNotBroadenSkippedDeletesOrUnmatchedIdentity(t *testing.T) {
	handler := queueHandler(t)
	handler.metadata.Root.Relations[0].Child.OnDeleteNotFound = "ignore"
	in := queueInput(true)
	in.Rows = in.Rows[:1]
	in.CurrentRows = in.CurrentRows[:1]
	in.CurrentChildren = nil
	in.Rows[0].Children[0].Remove = true
	in.Rows[0].Children[0].Has.Remove = true
	probe := &queueProbe{}
	caps := &queueCaps{missingDML: true}
	program, err := executeQueue(t, context.Background(), handler, in, probe, caps)
	if err != nil || len(program.actions.Rows) != 0 || len(probe.events) != 2 || probe.events[0].Location != "Rows[0]" {
		t.Fatal("skipped delete became queue attempt", err, probe.events)
	}
	assigned, err := New(assignedComponent(assignedUpdateIdentity), reflect.TypeFor[assignedInput](), reflect.TypeFor[assignedOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	assigned.metadata.Root.HookType = reflect.TypeFor[queueRecorder]()
	input := &assignedInput{Rows: []*assignedRow{{ID: ptr(999), Name: ptr("unmatched"), Has: &assignedHas{ID: true, Name: true}}}}
	snapshot, err := assigned.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	caps = &queueCaps{missingDML: true}
	_, err = assigned.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: caps})
	events := snapshot.(*Program).hook.Interface().(*queueRecorder).events
	if err != nil || len(events) != 0 || caps.starts != 0 || caps.allocations != 0 || len(caps.writes) != 0 || *input.Rows[0].ID != 999 {
		t.Fatal("unmatched identity became queue attempt", err)
	}
}
func TestQueueAttemptNoopPreservesConcurrencyToken(t *testing.T) {
	handler, err := New(assignedComponent(""), reflect.TypeFor[assignedVersionInput](), reflect.TypeFor[assignedVersionOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.HookType = reflect.TypeFor[queueRecorder]()
	input := &assignedVersionInput{Rows: []*assignedVersionRow{{ID: ptr(1), Version: ptr(7), Has: &assignedVersionHas{ID: true, Version: true}}}, CurrentRows: []*assignedVersionRow{{ID: ptr(1), Name: ptr("old"), Version: ptr(7)}}}
	snapshot, err := handler.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	caps := &queueCaps{missingDML: true}
	_, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: caps})
	events := snapshot.(*Program).hook.Interface().(*queueRecorder).events
	if err != nil || len(events) != 2 || events[1].Result != h.QueueNoopCompleted || caps.starts != 0 || *input.Rows[0].Version != 7 || input.Rows[0].Has.Name || !input.Rows[0].Has.Version {
		t.Fatal("noop advanced token or presence", err)
	}
}

type queueProjectedCurrent struct{ ID *int }
type queueProjectedInput struct {
	Rows        []*sqPlainParent         `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
	CurrentRows []*queueProjectedCurrent `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`
}

func TestQueueAttemptPreviousFieldsTrackActualProjection(t *testing.T) {
	handler, err := New(sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", ""), reflect.TypeFor[queueProjectedInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.HookType = reflect.TypeFor[queueRecorder]()
	input := &queueProjectedInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}}, CurrentRows: []*queueProjectedCurrent{{ID: ptr(1)}}}
	snapshot, err := handler.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &queueCaps{missingDML: true}})
	if err != nil {
		t.Fatal(err)
	}
	events := snapshot.(*Program).hook.Interface().(*queueRecorder).events
	if len(events) != 2 || !events[0].PreviousFields.Has("ID") || events[0].PreviousFields.Has("Name") {
		t.Fatal("unloaded optional field reported as known-null")
	}
}

func TestQueueAttemptFollowsNativeSiblingDependencyOrder(t *testing.T) {
	parent := &sqPlainParent{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}
	child := &sqPlainChild{ID: ptr(11), ParentID: ptr(1), Has: &sqPlainChildHas{ID: true, ParentID: true}}
	parents := &Record{Path: "parents", Table: "parents", EntityType: reflect.TypeFor[sqPlainParent](), Fields: []Field{{Name: "ID", Column: "id", Index: []int{0}}}}
	children := &Record{Path: "children", Table: "children", EntityType: reflect.TypeFor[sqPlainChild](), Fields: []Field{{Name: "ID", Column: "id", Index: []int{0}}, {Name: "ParentID", Column: "parent_id", Index: []int{1}, RefTable: "parents", RefColumn: "id"}}}
	parentFrame := &Frame{Entity: reflect.ValueOf(parent), Record: parents, Action: h.WriteInsert, Location: "Rows[1]", Fields: livePresence(parents, reflect.ValueOf(parent).Elem()), Original: originalPresence{presence: snapshotPresence(parents, reflect.ValueOf(parent).Elem())}}
	childFrame := &Frame{Entity: reflect.ValueOf(child), Record: children, Action: h.WriteInsert, Location: "Rows[0]", Fields: livePresence(children, reflect.ValueOf(child).Elem()), Original: originalPresence{presence: snapshotPresence(children, reflect.ValueOf(child).Elem())}}
	recorder := &queueRecorder{}
	program := &Program{metadata: &Metadata{Root: parents}, hook: reflect.ValueOf(recorder), frames: &MutationFrames{Rows: []*Frame{childFrame, parentFrame}}, actions: &MutationActions{}}
	if err := program.orderFramesByReferences(); err != nil {
		t.Fatal(err)
	}
	for _, frame := range program.frames.Rows {
		action := &Action{Entity: frame.Entity, Kind: frame.Action, frame: frame}
		program.actions.Rows = append(program.actions.Rows, action)
		program.queueItems = append(program.queueItems, action)
	}
	caps := &queueCaps{}
	if err := program.queue(context.Background(), caps); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(caps.writes, []string{"insert:parents:1", "insert:children:11"}) || len(recorder.events) != 4 || recorder.events[0].Location != "Rows[1]" || recorder.events[2].Location != "Rows[0]" {
		t.Fatal("observation rearranged dependencies", caps.writes, recorder.events)
	}
}

type queueOpaqueRow struct {
	ID     int
	Opaque chan int
}

func TestQueueAttemptEvidenceFailureDoesNotControlWork(t *testing.T) {
	row := &queueOpaqueRow{ID: 1, Opaque: make(chan int)}
	record := &Record{Path: "opaque", Table: "opaque", EntityType: reflect.TypeFor[queueOpaqueRow](), Fields: []Field{{Name: "ID", Index: []int{0}}}}
	frame := &Frame{Record: record, Entity: reflect.ValueOf(row), Location: "Rows[0]", Fields: livePresence(record, reflect.ValueOf(row).Elem()), Original: originalPresence{presence: snapshotPresence(record, reflect.ValueOf(row).Elem())}}
	recorder := &queueRecorder{}
	action := &Action{Kind: h.WriteInsert, Entity: frame.Entity, frame: frame}
	program := &Program{metadata: &Metadata{Root: record}, hook: reflect.ValueOf(recorder), frames: &MutationFrames{Rows: []*Frame{frame}}, actions: &MutationActions{Rows: []*Action{action}}, queueItems: []*Action{action}}
	caps := &queueCaps{}
	if err := program.queue(context.Background(), caps); err != nil {
		t.Fatal(err)
	}
	if len(caps.writes) != 1 || len(recorder.events) != 2 || recorder.events[0].EvidenceError == nil || recorder.events[0].Row != nil || recorder.events[1].Result != h.QueueQueued || !recorder.events[1].Queued {
		t.Fatal("evidence failure changed work")
	}
}

func (c *queueCaps) Flush(context.Context, string) error { c.flushes++; return nil }
func TestQueueAttemptImperativeNoopDoesNotFlush(t *testing.T) {
	ctx := handlerengine.PrepareComponent(context.Background(), handlerengine.ComponentImperative, "1")
	for _, physical := range []bool{false, true} {
		in := queueInput(physical == false)
		if !physical {
			in.Rows = in.Rows[:1]
			in.CurrentRows = in.CurrentRows[:1]
			in.Rows[0].Children[0].Label = nil
			in.Rows[0].Children[0].Has.Label = false
		}
		caps := &queueCaps{}
		_, err := executeQueue(t, ctx, queueHandler(t), in, &queueProbe{}, caps)
		expected := 0
		if physical {
			expected = 1
		}
		if err != nil || caps.flushes != expected {
			t.Fatal("imperative flush boundary changed", physical, caps.flushes, err)
		}
	}
	// Existing non-observed imperative execution keeps its prior flush behavior.
	handler := queueHandler(t)
	handler.metadata.Root.HookType = nil
	caps := &queueCaps{}
	if _, err := handler.Execute(ctx, rhandler.Invocation{Input: &sqPlainInput{}, Binder: caps}); err != nil || caps.flushes != 1 {
		t.Fatal("default imperative flush changed", err, caps.flushes)
	}
}
