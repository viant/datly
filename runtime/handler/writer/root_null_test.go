package writer

import (
	"context"
	"errors"
	rhandler "github.com/viant/datly/runtime/handler"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type nullRootProbe struct {
	observedWriterProbe
	inits     int
	originals []bool
}

func (p *nullRootProbe) Init(_ context.Context, row *sqPlainParent, state h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	p.inits++
	p.originals = append(p.originals, state.Original.Has("Name"))
	row.Has.Name = true
	return nil
}

func TestRootNullInitialValidationBoundaries(t *testing.T) {
	for _, indexes := range [][]int{{0}, {1}, {2}, {0, 2}} {
		for _, enabled := range []bool{false, true} {
			in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}, {ID: ptr(2), Has: &sqPlainParentHas{ID: true}}, {ID: ptr(3), Has: &sqPlainParentHas{ID: true}}}}
			for _, i := range indexes {
				in.Rows[i] = nil
			}
			component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
			if enabled {
				component.RootView.RootNullPolicy = "initial-validation"
			}
			handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			handler.metadata.Root.HookType = reflect.TypeFor[nullRootProbe]()
			observer := handler.NewPhaseObserver().(*nullRootProbe)
			ctx, _ := rhandler.WithPhaseObserver(context.Background(), observer, 21, 0)
			snapshot, err := handler.CaptureInput(ctx, in)
			if !enabled {
				if err == nil || len(observer.events) != 0 {
					t.Fatal("default null rejection moved")
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			caps := &finalValidationCapabilities{}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: caps})
			var structural *h.RootNullRecordError
			if !errors.As(err, &structural) || structural.Location != "Rows["+string(rune('0'+indexes[0]))+"]" || structural.Cause == nil || !errors.Is(err, structural.Cause) {
				t.Fatalf("structural error=%v", err)
			}
			if observer.calls != 0 || caps.passes != 0 || caps.starts != 0 || caps.allocations != 0 || caps.inserts+caps.updates+caps.deletes != 0 {
				t.Fatal("structural failure reached validation or mutation")
			}
			if observer.inits != 3-len(indexes) {
				t.Fatal("invalid row hook or missing valid row hook")
			}
			for _, original := range observer.originals {
				if original {
					t.Fatal("original captured after Init")
				}
			}
			want := []h.InvocationPhase{h.PhaseExecution, h.PhaseInitialization, h.PhaseInitialization, h.PhaseValidation, h.PhaseValidation, h.PhaseExecution}
			if len(observer.events) != len(want) {
				t.Fatalf("events=%v", observer.events)
			}
			for i, e := range observer.events {
				if e.Phase != want[i] {
					t.Fatalf("events=%v", observer.events)
				}
			}
			if observer.events[4].Result != h.PhaseFailed {
				t.Fatal("structural error became violations")
			}
			for _, i := range indexes {
				if in.Rows[i] != nil {
					t.Fatal("null substituted")
				}
			}
		}
	}
}

func TestRootNullCaptureRetainedAndInitIntroduced(t *testing.T) {
	for _, captured := range []bool{false, true} {
		component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
		component.RootView.RootNullPolicy = "initial-validation"
		handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
		if err != nil {
			t.Fatal(err)
		}
		in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}}}
		if captured {
			in.Rows[0] = nil
		}
		snapshot, err := handler.CaptureInput(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		// Input.Init runs between capture and frame construction.
		if captured {
			in.Rows[0] = &sqPlainParent{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}
		} else {
			in.Rows[0] = nil
		}
		_, err = handler.Execute(context.Background(), rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &finalValidationCapabilities{}})
		var structural *h.RootNullRecordError
		if !errors.As(err, &structural) || structural.Location != "Rows[0]" {
			t.Fatal(err)
		}
	}
}

func TestRootNullControlsAndNestedEarlyRejection(t *testing.T) {
	component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
	component.RootView.RootNullPolicy = "initial-validation"
	handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]*sqPlainParent{nil, {}, {{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}}} {
		in := &sqPlainInput{Rows: rows}
		snapshot, err := handler.CaptureInput(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		_, err = handler.Execute(context.Background(), rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &finalValidationCapabilities{}})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, rows := range [][]*sqPlainParent{{{Children: []*sqPlainChild{nil}}}, {nil, {Children: []*sqPlainChild{nil}}}} {
		_, err = handler.CaptureInput(context.Background(), &sqPlainInput{Rows: rows})
		var structural *h.RootNullRecordError
		if err == nil || errors.As(err, &structural) {
			t.Fatal("nested null lost early precedence")
		}
	}
	component.RootView.Relations[0].View.RootNullPolicy = "initial-validation"
	if _, err = New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch"); err == nil {
		t.Fatal("nested null policy accepted")
	}
	component.RootView.Relations[0].View.RootNullPolicy = ""
	component.RootView.RootNullPolicy = "invalid"
	if _, err = New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch"); err == nil {
		t.Fatal("invalid root policy accepted")
	}
}

func TestRootNullToOneBoundaryAndBindingPrecedence(t *testing.T) {
	root := &Record{Path: "root", EntityType: reflect.TypeFor[diagnosticParent](), RootNullPolicy: "initial-validation"}
	one := &Record{Path: "root/One", EntityType: reflect.TypeFor[diagnosticChild]()}
	root.Relations = []*Relation{{Child: one, Field: []int{1}}}
	in := &diagnosticInput{Payload: []*diagnosticParent{{One: nil}}}
	program := &Program{input: in, metadata: &Metadata{Root: root, InputField: 0, Operation: "post"}, original: &OriginalInput{Presence: map[uintptr]originalPresence{}}, database: &DatabaseSnapshot{Rows: map[rowIdentity]reflect.Value{}}, frames: &MutationFrames{}}
	if err := program.captureOriginal(root, reflect.ValueOf(in.Payload)); err != nil {
		t.Fatal(err)
	}
	if err := program.buildRecordFrames(context.Background(), nil, root, reflect.ValueOf(in.Payload), nil); err != nil || program.structuralError != nil || len(program.frames.Rows) != 1 {
		t.Fatal("optional to-one policy changed", err)
	}
	if err := validateRootNullPolicy(root, reflect.TypeFor[*diagnosticParent]()); err == nil {
		t.Fatal("to-one root policy accepted")
	}
	handler := queueHandler(t)
	handler.metadata.Root.RootNullPolicy = "initial-validation"
	snapshot, err := handler.CaptureInput(context.Background(), &sqPlainInput{Rows: []*sqPlainParent{nil}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = handler.Execute(context.Background(), rhandler.Invocation{Input: snapshot.(*Program).input, Snapshot: snapshot, Binder: nil})
	var structural *h.RootNullRecordError
	if err == nil || errors.As(err, &structural) {
		t.Fatal("binding failure precedence moved", err)
	}
}

type nullIntroducingProbe struct {
	observedWriterProbe
	input  *sqPlainInput
	inits  int
	nested bool
}

func (p *nullIntroducingProbe) Init(_ context.Context, _ *sqPlainParent, state h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	p.inits++
	if state.Location == "Rows[0]" {
		if p.nested {
			p.input.Rows[0].Children[0] = nil
		} else {
			p.input.Rows[1] = nil
		}
	}
	return nil
}
func TestRootNullIntroducedByEntityInitSkipsInvalidFrameHook(t *testing.T) {
	for _, nested := range []bool{false, true} {
		component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
		component.RootView.RootNullPolicy = "initial-validation"
		handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
		if err != nil {
			t.Fatal(err)
		}
		handler.metadata.Root.HookType = reflect.TypeFor[nullIntroducingProbe]()
		in := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}, {ID: ptr(2), Has: &sqPlainParentHas{ID: true}}}}
		if nested {
			in.Rows[0].Children = []*sqPlainChild{{ID: ptr(11), ParentID: ptr(1), Has: &sqPlainChildHas{ID: true, ParentID: true}}}
		}
		observer := handler.NewPhaseObserver().(*nullIntroducingProbe)
		observer.input = in
		observer.nested = nested
		ctx, _ := rhandler.WithPhaseObserver(context.Background(), observer, 1, 0)
		snapshot, err := handler.CaptureInput(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		caps := &finalValidationCapabilities{}
		_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: caps})
		var structural *h.RootNullRecordError
		if nested {
			if err == nil || errors.As(err, &structural) || observer.events[len(observer.events)-2].Phase != h.PhaseInitialization {
				t.Fatal("nested rejection moved to validation", err)
			}
		} else {
			if !errors.As(err, &structural) || structural.Location != "Rows[1]" || observer.inits != 1 {
				t.Fatal("invalid frame received Init or diagnostic lost", err, observer.inits)
			}
		}
		if observer.calls != 0 || caps.passes != 0 || caps.starts != 0 || caps.allocations != 0 || caps.inserts+caps.updates+caps.deletes != 0 {
			t.Fatal("invalid graph reached business validation or mutations")
		}
	}
}

type defaultNullHookProbe struct {
	called  bool
	failure error
}

func (p *defaultNullHookProbe) Init(_ context.Context, row *sqPlainParent, _ h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	p.called = true
	if row == nil {
		return p.failure
	}
	return nil
}
func TestRootNullGuardDoesNotChangeNonOptedHookDispatch(t *testing.T) {
	probe := &defaultNullHookProbe{failure: errors.New("existing hook error")}
	root := &Record{EntityType: reflect.TypeFor[sqPlainParent]()}
	program := &Program{metadata: &Metadata{Root: root}, output: &sqPlainOutput{}}
	frame := &Frame{Record: root, Entity: reflect.ValueOf((*sqPlainParent)(nil)), Hook: reflect.ValueOf(probe)}
	if err := program.callEntityHook(context.Background(), "Init", frame); !probe.called || err != probe.failure {
		t.Fatal("non-opted hook/error behavior changed", err)
	}
}
