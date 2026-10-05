package writer

import (
	"context"
	"errors"
	"fmt"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

func nestedHandler(t *testing.T, rootPolicy bool) *Handler {
	t.Helper()
	component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
	component.RootView.Relations[0].View.NestedNullPolicy = "initial-validation"
	if rootPolicy {
		component.RootView.RootNullPolicy = "initial-validation"
	}
	handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	handler.metadata.Root.HookType = reflect.TypeFor[nullRootProbe]()
	return handler
}

func TestNestedNullCaptureLocationsAndValidationBoundary(t *testing.T) {
	for _, positions := range [][]int{{0}, {1}, {2}, {0, 2}} {
		t.Run(fmt.Sprint(positions), func(t *testing.T) {
			handler := nestedHandler(t, false)
			in := queueInput(false)
			in.Rows[1].Children = []*sqPlainChild{{ID: ptr(21), ParentID: ptr(2), Has: &sqPlainChildHas{ID: true}}, {ID: ptr(22), ParentID: ptr(2), Has: &sqPlainChildHas{ID: true}}, {ID: ptr(23), ParentID: ptr(2), Has: &sqPlainChildHas{ID: true}}}
			for _, i := range positions {
				in.Rows[1].Children[i] = nil
			}
			observer := handler.NewPhaseObserver().(*nullRootProbe)
			ctx, _ := rhandler.WithPhaseObserver(context.Background(), observer, 55, 0)
			snapshot, err := handler.CaptureInput(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			caps := &finalValidationCapabilities{}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: caps})
			var structural *h.NestedNullRecordError
			want := fmt.Sprintf("Rows[1].Children[%d]", positions[0])
			if !errors.As(err, &structural) || structural.Location != want || structural.Cause == nil || !errors.Is(err, structural.Cause) {
				t.Fatalf("error=%v want=%s", err, want)
			}
			if observer.calls != 0 || caps.passes != 0 || caps.starts != 0 || caps.allocations != 0 || caps.inserts+caps.updates+caps.deletes != 0 {
				t.Fatal("structural failure reached business validation or mutation")
			}
			if len(observer.events) != 6 || observer.events[3].Phase != h.PhaseValidation || observer.events[4].Result != h.PhaseFailed {
				t.Fatal("incorrect actual phase sequence", observer.events)
			}
			for _, i := range positions {
				if in.Rows[1].Children[i] != nil {
					t.Fatal("null replaced")
				}
			}
		})
	}
}

func TestNestedNullCapturedPrecedenceAndInitChanges(t *testing.T) {
	for _, captured := range []bool{true, false} {
		handler := nestedHandler(t, false)
		in := queueInput(false)
		if captured {
			in.Rows[0].Children[0] = nil
		}
		snapshot, err := handler.CaptureInput(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		if captured {
			in.Rows[0].Children = nil
			in.Rows[1].Children[0] = nil
		} else {
			in.Rows[1].Children[0] = nil
		}
		_, err = handler.Execute(context.Background(), rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &finalValidationCapabilities{}})
		var structural *h.NestedNullRecordError
		want := "Rows[1].Children[0]"
		if captured {
			want = "Rows[0].Children[0]"
		}
		if !errors.As(err, &structural) || structural.Location != want {
			t.Fatal("captured location lost", err)
		}
	}
	for _, rootFirst := range []bool{true, false} {
		handler := nestedHandler(t, true)
		in := queueInput(false)
		in.Rows[0].Children[0] = nil
		if rootFirst {
			in.Rows = append([]*sqPlainParent{nil}, in.Rows...)
		} else {
			in.Rows = append(in.Rows, nil)
		}
		snapshot, err := handler.CaptureInput(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		_, err = handler.Execute(context.Background(), rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: &finalValidationCapabilities{}})
		var root *h.RootNullRecordError
		var nested *h.NestedNullRecordError
		if rootFirst {
			if !errors.As(err, &root) || root.Location != "Rows[0]" {
				t.Fatal(err)
			}
		} else {
			if !errors.As(err, &nested) || nested.Location != "Rows[0].Children[0]" {
				t.Fatal(err)
			}
		}
	}
	handler := nestedHandler(t, false)
	in := queueInput(false)
	in.Rows[0].Children[0] = nil
	in.Rows = append(in.Rows, nil)
	if _, err := handler.CaptureInput(context.Background(), in); err == nil {
		t.Fatal("non-opted root rejection deferred")
	}
}

func TestNestedNullEntityInitAndTargetRestrictions(t *testing.T) {
	handler := nestedHandler(t, false)
	handler.metadata.Root.HookType = reflect.TypeFor[nullIntroducingProbe]()
	in := queueInput(false)
	observer := handler.NewPhaseObserver().(*nullIntroducingProbe)
	observer.input = in
	observer.nested = true
	ctx, _ := rhandler.WithPhaseObserver(context.Background(), observer, 1, 0)
	snapshot, err := handler.CaptureInput(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	caps := &finalValidationCapabilities{}
	_, err = handler.Execute(ctx, rhandler.Invocation{Input: in, Snapshot: snapshot, Binder: caps})
	var structural *h.NestedNullRecordError
	if !errors.As(err, &structural) || structural.Location != "Rows[0].Children[0]" || observer.events[len(observer.events)-2].Phase != h.PhaseValidation {
		t.Fatal("Init invalidation not deferred", err)
	}
	if caps.starts+caps.allocations+caps.inserts+caps.updates+caps.deletes != 0 || observer.calls != 0 {
		t.Fatal("invalid Init graph mutated")
	}
	for _, kind := range []string{"root", "auxiliary", "unknown"} {
		component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
		child := component.RootView.Relations[0].View
		child.NestedNullPolicy = "initial-validation"
		switch kind {
		case "root":
			component.RootView.NestedNullPolicy = "initial-validation"
		case "auxiliary":
			child.Auxiliary = true
		case "unknown":
			child.NestedNullPolicy = "bad"
		}
		if _, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch"); err == nil {
			t.Fatal("invalid target accepted", kind)
		}
	}
}

func TestNestedNullPolicyGeneratedTagCarrier(t *testing.T) {
	component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
	for _, view := range []*spec.View{nil, {Name: "Children"}} {
		record, err := compileRecord(component, reflect.TypeFor[sqPlainInput](), "Children", "Rows/Children", reflect.TypeFor[sqPlainChild](), view, "Children,table=children,nestedNullPolicy=initial-validation")
		if err != nil || record.NestedNullPolicy != "initial-validation" || record.RootNullPolicy != "" {
			t.Fatalf("generated tag lost when relation metadata is absent: %v", err)
		}
	}
}
