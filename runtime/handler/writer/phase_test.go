package writer

import (
	"context"
	"errors"
	rhandler "github.com/viant/datly/runtime/handler"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type observedWriterProbe struct {
	passingAggregateProbe
	events []h.PhaseEvent
}

func (p *observedWriterProbe) ObservePhase(_ context.Context, event h.PhaseEvent) {
	p.events = append(p.events, event)
}
func TestWriterObserverReusesRootAndReportsActualPhases(t *testing.T) {
	for _, tc := range []struct {
		name        string
		empty, fail bool
	}{{"insert", false, false}, {"empty", true, false}, {"validator operational failure", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			handler, err := New(sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", ""), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			handler.metadata.Root.HookType = reflect.TypeFor[observedWriterProbe]()
			observer := handler.NewPhaseObserver().(*observedWriterProbe)
			ctx, _ := rhandler.WithPhaseObserver(context.Background(), observer, 31, 2)
			input := &sqPlainInput{}
			if !tc.empty {
				input.Rows = []*sqPlainParent{{Name: ptr("new"), Has: &sqPlainParentHas{Name: true}}}
			}
			snapshot, err := handler.CaptureInput(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.(*Program).hook.Interface() != observer {
				t.Fatal("root observer was replaced during capture")
			}
			caps := &finalValidationCapabilities{}
			var binder h.Binder = caps
			if tc.fail {
				binder = &aggregateCapabilities{operational: errors.New("validator unavailable")}
			}
			_, err = handler.Execute(ctx, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: binder})
			if (err != nil) != tc.fail {
				t.Fatalf("error=%v", err)
			}
			want := []h.InvocationPhase{h.PhaseExecution, h.PhaseInitialization, h.PhaseInitialization, h.PhaseValidation, h.PhaseValidation}
			if !tc.fail {
				if !tc.empty {
					want = append(want, h.PhaseAllocation, h.PhaseAllocation)
				}
				want = append(want, h.PhaseQueue, h.PhaseQueue)
			}
			want = append(want, h.PhaseExecution)
			if len(observer.events) != len(want) {
				t.Fatalf("events=%+v", observer.events)
			}
			for i, event := range observer.events {
				boundary := h.PhaseBegin
				if i == len(want)-1 || i > 0 && i%2 == 0 {
					boundary = h.PhaseEnd
				}
				if event.Phase != want[i] || event.Boundary != boundary || event.InvocationID != 31 || event.Attempt != 2 {
					t.Fatalf("event[%d]=%+v", i, event)
				}
			}
			if tc.fail && observer.events[len(want)-1].Result != h.PhaseFailed {
				t.Fatal("operational failure misclassified")
			}
			if tc.empty && (caps.inserts != 0 || caps.allocations != 0) {
				t.Fatal("empty input mutated")
			}
		})
	}
}
